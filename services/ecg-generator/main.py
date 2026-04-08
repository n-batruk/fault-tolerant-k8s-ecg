import asyncio
import logging
import math
import os
import uuid
from datetime import datetime, timedelta, timezone

import grpc

import ecg_ingestion_pb2
import ecg_ingestion_pb2_grpc


logging.basicConfig(
    level=(os.getenv("LOG_LEVEL") or "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger("ecg-generator")


def to_iso_utc(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_samples(
    sequence_from: int,
    samples_count: int,
    sampling_rate: int,
) -> list[float]:
    amplitude = 1.0
    frequency_hz = 1.0

    samples = []

    for index in range(samples_count):
        sample_index = sequence_from + index
        t = sample_index / sampling_rate

        value = amplitude * math.sin(2.0 * math.pi * frequency_hz * t)
        samples.append(round(value, 6))

    return samples


def make_chunk(
    index: int,
    session_id: str,
    source_id: str,
    sampling_rate: int,
    lead_id: str,
    chunk_duration_seconds: int,
    base_time: datetime,
    skip_chunk_index: int,
) -> ecg_ingestion_pb2.EcgChunk:
    samples_count = sampling_rate * chunk_duration_seconds

    effective_index = index
    if skip_chunk_index >= 0 and index >= skip_chunk_index:
        effective_index = index + 1

    sequence_from = effective_index * samples_count
    sequence_to = sequence_from + samples_count - 1

    timestamp_from = base_time + timedelta(seconds=effective_index * chunk_duration_seconds)
    timestamp_to = timestamp_from + timedelta(seconds=chunk_duration_seconds)

    samples = build_samples(
        sequence_from=sequence_from,
        samples_count=samples_count,
        sampling_rate=sampling_rate,
    )

    return ecg_ingestion_pb2.EcgChunk(
        schema_version="1.0",
        source_id=source_id,
        session_id=session_id,
        sequence_from=sequence_from,
        sequence_to=sequence_to,
        timestamp_from=to_iso_utc(timestamp_from),
        timestamp_to=to_iso_utc(timestamp_to),
        sampling_rate=sampling_rate,
        lead_id=lead_id,
        samples=samples,
    )


async def request_generator():
    source_id = os.getenv("SOURCE_ID") or "mock-device-001"

    session_id = os.getenv("SESSION_ID")
    if session_id is None or session_id == "":
        session_id = f"mock-session-{uuid.uuid4()}"

    sampling_rate = int(os.getenv("SAMPLING_RATE") or "500")
    lead_id = os.getenv("LEAD_ID") or "II"
    chunks_count = int(os.getenv("CHUNKS_COUNT") or "100")
    chunk_duration_seconds = int(os.getenv("CHUNK_DURATION_SECONDS") or "1")
    send_interval_seconds = float(os.getenv("SEND_INTERVAL_SECONDS") or "0.05")
    skip_chunk_index = int(os.getenv("SKIP_CHUNK_INDEX") or "-1")

    base_time = datetime.now(timezone.utc)

    logger.info(
        "starting generation session_id=%s source_id=%s chunks_count=%s sampling_rate=%s lead_id=%s",
        session_id,
        source_id,
        chunks_count,
        sampling_rate,
        lead_id,
    )

    for index in range(chunks_count):
        chunk = make_chunk(
            index=index,
            session_id=session_id,
            source_id=source_id,
            sampling_rate=sampling_rate,
            lead_id=lead_id,
            chunk_duration_seconds=chunk_duration_seconds,
            base_time=base_time,
            skip_chunk_index=skip_chunk_index,
        )

        logger.info(
            "sending chunk session_id=%s sequence_from=%s sequence_to=%s timestamp_from=%s timestamp_to=%s",
            chunk.session_id,
            chunk.sequence_from,
            chunk.sequence_to,
            chunk.timestamp_from,
            chunk.timestamp_to,
        )

        yield chunk
        await asyncio.sleep(send_interval_seconds)


async def main():
    grpc_target = (
        os.getenv("GRPC_TARGET")
        or "grpc-stream-adapter.ecg-system.svc.cluster.local:50051"
    )

    logger.info("connecting to grpc target=%s", grpc_target)

    async with grpc.aio.insecure_channel(grpc_target) as channel:
        stub = ecg_ingestion_pb2_grpc.EcgIngestionStub(channel)

        accepted = 0
        rejected = 0

        response_stream = stub.StreamChunks(request_generator())

        async for ack in response_stream:
            if ack.accepted:
                accepted += 1
            else:
                rejected += 1

            logger.info(
                "ack session_id=%s sequence_from=%s sequence_to=%s accepted=%s message=%s",
                ack.session_id,
                ack.sequence_from,
                ack.sequence_to,
                ack.accepted,
                ack.message,
            )

        logger.info(
            "generation finished accepted=%s rejected=%s",
            accepted,
            rejected,
        )


if __name__ == "__main__":
    asyncio.run(main())
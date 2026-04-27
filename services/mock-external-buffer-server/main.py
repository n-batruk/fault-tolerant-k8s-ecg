import asyncio
import logging
import math
import os
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Deque

from fastapi import FastAPI, Query
from pydantic import BaseModel


logging.basicConfig(
    level=(os.getenv("LOG_LEVEL") or "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger("mock-external-buffer-server")

app = FastAPI(
    title="Mock External ECG Buffer Server",
    version="1.0.0",
)


class EcgChunk(BaseModel):
    schema_version: str
    source_id: str
    session_id: str
    sequence_from: int
    sequence_to: int
    timestamp_from: str
    timestamp_to: str
    sampling_rate: int
    lead_id: str
    samples: list[float]


class EventsResponse(BaseModel):
    source_id: str
    session_id: str
    chunks: list[EcgChunk]
    has_more: bool
    buffer_oldest_sequence_from: int | None
    buffer_newest_sequence_to: int | None


source_id = os.getenv("SOURCE_ID") or "mock-external-buffer-server-001"
session_id = os.getenv("SESSION_ID") or "longpoll-session-001"
sampling_rate = int(os.getenv("SAMPLING_RATE") or "500")
lead_id = os.getenv("LEAD_ID") or "II"
chunk_duration_seconds = int(os.getenv("CHUNK_DURATION_SECONDS") or "1")
generation_interval_seconds = float(os.getenv("GENERATION_INTERVAL_SECONDS") or "1.0")
buffer_chunks = int(os.getenv("BUFFER_CHUNKS") or "100")
preload_chunks = int(os.getenv("PRELOAD_CHUNKS") or "30")

buffer: Deque[EcgChunk] = deque(maxlen=buffer_chunks)
base_time = datetime.now(timezone.utc)
next_chunk_index = 0
buffer_condition = asyncio.Condition()


def to_iso_utc(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_samples(
    sequence_from: int,
    samples_count: int,
    sample_rate: int,
) -> list[float]:
    amplitude = 1.0
    frequency_hz = 1.0

    samples = []

    for index in range(samples_count):
        sample_index = sequence_from + index
        t = sample_index / sample_rate
        value = amplitude * math.sin(2.0 * math.pi * frequency_hz * t)
        samples.append(round(value, 6))

    return samples


def make_chunk(index: int) -> EcgChunk:
    samples_count = sampling_rate * chunk_duration_seconds

    sequence_from = index * samples_count
    sequence_to = sequence_from + samples_count - 1

    timestamp_from = base_time + timedelta(seconds=index * chunk_duration_seconds)
    timestamp_to = timestamp_from + timedelta(seconds=chunk_duration_seconds)

    return EcgChunk(
        schema_version="1.0",
        source_id=source_id,
        session_id=session_id,
        sequence_from=sequence_from,
        sequence_to=sequence_to,
        timestamp_from=to_iso_utc(timestamp_from),
        timestamp_to=to_iso_utc(timestamp_to),
        sampling_rate=sampling_rate,
        lead_id=lead_id,
        samples=build_samples(
            sequence_from=sequence_from,
            samples_count=samples_count,
            sample_rate=sampling_rate,
        ),
    )


async def add_chunk_to_buffer(chunk: EcgChunk) -> None:
    async with buffer_condition:
        buffer.append(chunk)
        buffer_condition.notify_all()


async def generator_loop() -> None:
    global next_chunk_index

    logger.info(
        "starting buffer generator source_id=%s session_id=%s buffer_chunks=%s",
        source_id,
        session_id,
        buffer_chunks,
    )

    for _ in range(preload_chunks):
        chunk = make_chunk(next_chunk_index)
        await add_chunk_to_buffer(chunk)
        next_chunk_index += 1

    logger.info("preloaded chunks=%s", preload_chunks)

    while True:
        await asyncio.sleep(generation_interval_seconds)

        chunk = make_chunk(next_chunk_index)
        await add_chunk_to_buffer(chunk)

        logger.info(
            "generated chunk session_id=%s sequence_from=%s sequence_to=%s buffer_size=%s",
            chunk.session_id,
            chunk.sequence_from,
            chunk.sequence_to,
            len(buffer),
        )

        next_chunk_index += 1


@app.on_event("startup")
async def startup() -> None:
    asyncio.create_task(generator_loop())


@app.get("/healthz")
async def healthz() -> dict:
    return {
        "status": "ok",
        "service": "mock-external-buffer-server",
        "source_id": source_id,
        "session_id": session_id,
        "buffer_size": len(buffer),
    }


@app.get("/api/v1/ecg/events", response_model=EventsResponse)
async def get_events(
    requested_session_id: str = Query(alias="session_id"),
    after_sequence: int = Query(default=-1),
    limit: int = Query(default=10, ge=1, le=100),
    wait_timeout_seconds: float = Query(default=10.0, ge=0.0, le=60.0),
) -> EventsResponse:
    if requested_session_id != session_id:
        return EventsResponse(
            source_id=source_id,
            session_id=requested_session_id,
            chunks=[],
            has_more=False,
            buffer_oldest_sequence_from=None,
            buffer_newest_sequence_to=None,
        )

    deadline = asyncio.get_running_loop().time() + wait_timeout_seconds

    while True:
        matching_chunks = [
            chunk for chunk in list(buffer)
            if chunk.sequence_from > after_sequence
        ]

        if matching_chunks:
            selected = matching_chunks[:limit]
            has_more = len(matching_chunks) > len(selected)

            oldest = buffer[0].sequence_from if buffer else None
            newest = buffer[-1].sequence_to if buffer else None

            logger.info(
                "returning chunks session_id=%s after_sequence=%s count=%s",
                session_id,
                after_sequence,
                len(selected),
            )

            return EventsResponse(
                source_id=source_id,
                session_id=session_id,
                chunks=selected,
                has_more=has_more,
                buffer_oldest_sequence_from=oldest,
                buffer_newest_sequence_to=newest,
            )

        remaining = deadline - asyncio.get_running_loop().time()

        if remaining <= 0:
            oldest = buffer[0].sequence_from if buffer else None
            newest = buffer[-1].sequence_to if buffer else None

            return EventsResponse(
                source_id=source_id,
                session_id=session_id,
                chunks=[],
                has_more=False,
                buffer_oldest_sequence_from=oldest,
                buffer_newest_sequence_to=newest,
            )

        async with buffer_condition:
            try:
                await asyncio.wait_for(buffer_condition.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                pass


def run() -> None:
    import uvicorn

    host = os.getenv("HTTP_HOST") or "0.0.0.0"
    port = int(os.getenv("HTTP_PORT") or "8080")

    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        log_level=(os.getenv("LOG_LEVEL") or "INFO").lower(),
    )


if __name__ == "__main__":
    run()
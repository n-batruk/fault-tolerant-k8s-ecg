import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone

import grpc
from aiokafka import AIOKafkaProducer

import ecg_ingestion_pb2
import ecg_ingestion_pb2_grpc


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger("grpc-stream-adapter")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def validate_chunk(chunk: ecg_ingestion_pb2.EcgChunk) -> tuple[bool, str]:
    if chunk.schema_version == "":
        return False, "schema_version is required"

    if chunk.source_id == "":
        return False, "source_id is required"

    if chunk.session_id == "":
        return False, "session_id is required"

    if chunk.sequence_from < 0:
        return False, "sequence_from must be non-negative"

    if chunk.sequence_to < chunk.sequence_from:
        return False, "sequence_to must be greater than or equal to sequence_from"

    if chunk.timestamp_from == "":
        return False, "timestamp_from is required"

    if chunk.timestamp_to == "":
        return False, "timestamp_to is required"

    try:
        timestamp_from = parse_iso_datetime(chunk.timestamp_from)
    except ValueError:
        return False, "timestamp_from must be a valid ISO datetime"

    try:
        timestamp_to = parse_iso_datetime(chunk.timestamp_to)
    except ValueError:
        return False, "timestamp_to must be a valid ISO datetime"

    if timestamp_from.tzinfo is None:
        return False, "timestamp_from must include timezone"

    if timestamp_to.tzinfo is None:
        return False, "timestamp_to must include timezone"

    if timestamp_to <= timestamp_from:
        return False, "timestamp_to must be greater than timestamp_from"

    if chunk.sampling_rate <= 0:
        return False, "sampling_rate must be positive"

    if chunk.lead_id == "":
        return False, "lead_id is required"

    if len(chunk.samples) == 0:
        return False, "samples must not be empty"

    expected_count = chunk.sequence_to - chunk.sequence_from + 1
    if expected_count != len(chunk.samples):
        return False, "sequence range does not match samples count"

    return True, "accepted"


def build_rejected_event(
    chunk: ecg_ingestion_pb2.EcgChunk,
    reason: str,
) -> dict:
    return {
        "event_type": "ecg.chunk.rejected",
        "schema_version": chunk.schema_version,
        "source_id": chunk.source_id,
        "session_id": chunk.session_id,
        "sequence_from": chunk.sequence_from,
        "sequence_to": chunk.sequence_to,
        "reason": reason,
        "received_at": utc_now_iso(),
        "received_by": os.getenv("POD_NAME") or "unknown-pod",
    }


def build_publish_failed_event(
    chunk: ecg_ingestion_pb2.EcgChunk,
    reason: str,
) -> dict:
    return {
        "event_type": "ecg.chunk.publish_failed",
        "schema_version": chunk.schema_version,
        "source_id": chunk.source_id,
        "session_id": chunk.session_id,
        "sequence_from": chunk.sequence_from,
        "sequence_to": chunk.sequence_to,
        "reason": reason,
        "received_at": utc_now_iso(),
        "received_by": os.getenv("POD_NAME") or "unknown-pod",
    }


def build_ack(
    chunk: ecg_ingestion_pb2.EcgChunk,
    accepted: bool,
    message: str,
) -> ecg_ingestion_pb2.EcgAck:
    return ecg_ingestion_pb2.EcgAck(
        session_id=chunk.session_id,
        sequence_from=chunk.sequence_from,
        sequence_to=chunk.sequence_to,
        accepted=accepted,
        message=message,
    )


class EcgIngestionService(ecg_ingestion_pb2_grpc.EcgIngestionServicer):
    def __init__(
        self,
        producer: AIOKafkaProducer,
        topic: str,
        dead_letter_topic: str,
    ):
        self.producer = producer
        self.topic = topic
        self.dead_letter_topic = dead_letter_topic

    async def publish_dead_letter(
        self,
        chunk: ecg_ingestion_pb2.EcgChunk,
        event: dict,
    ) -> None:
        key = chunk.session_id.encode("utf-8") if chunk.session_id else b"unknown-session"

        await self.producer.send_and_wait(
            self.dead_letter_topic,
            key=key,
            value=json.dumps(
                event,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8"),
            headers=[
                ("content-type", b"application/json"),
                ("message-type", b"dead-letter-event"),
            ],
        )

    async def StreamChunks(self, request_iterator, context):
        try:
            async for chunk in request_iterator:
                accepted, message = validate_chunk(chunk)

                if not accepted:
                    rejected_event = build_rejected_event(chunk, message)

                    try:
                        await self.publish_dead_letter(chunk, rejected_event)

                        logger.warning(
                            "rejected chunk session_id=%s sequence_from=%s sequence_to=%s reason=%s",
                            chunk.session_id,
                            chunk.sequence_from,
                            chunk.sequence_to,
                            message,
                        )

                        yield build_ack(
                            chunk=chunk,
                            accepted=False,
                            message=message,
                        )

                    except Exception as exc:
                        logger.exception(
                            "failed to publish rejected chunk to dead-letter topic session_id=%s",
                            chunk.session_id,
                        )

                        yield build_ack(
                            chunk=chunk,
                            accepted=False,
                            message=f"{message}; dead-letter publish failed: {type(exc).__name__}",
                        )

                    continue

                try:
                    await self.producer.send_and_wait(
                        self.topic,
                        key=chunk.session_id.encode("utf-8"),
                        value=chunk.SerializeToString(),
                        headers=[
                            ("content-type", b"application/x-protobuf"),
                            ("message-type", b"EcgChunk"),
                            ("schema-version", chunk.schema_version.encode("utf-8")),
                        ],
                    )

                    logger.info(
                        "accepted chunk session_id=%s source_id=%s sequence_from=%s sequence_to=%s topic=%s",
                        chunk.session_id,
                        chunk.source_id,
                        chunk.sequence_from,
                        chunk.sequence_to,
                        self.topic,
                    )

                    yield build_ack(
                        chunk=chunk,
                        accepted=True,
                        message="published to kafka",
                    )

                except Exception as exc:
                    reason = f"kafka publish failed: {type(exc).__name__}"

                    logger.exception(
                        "failed to publish chunk to kafka session_id=%s sequence_from=%s sequence_to=%s",
                        chunk.session_id,
                        chunk.sequence_from,
                        chunk.sequence_to,
                    )

                    publish_failed_event = build_publish_failed_event(chunk, reason)

                    try:
                        await self.publish_dead_letter(chunk, publish_failed_event)
                    except Exception:
                        logger.exception(
                            "failed to publish kafka failure event to dead-letter topic session_id=%s",
                            chunk.session_id,
                        )

                    yield build_ack(
                        chunk=chunk,
                        accepted=False,
                        message=reason,
                    )

        except asyncio.CancelledError:
            logger.info("StreamChunks cancelled")
            raise

        except Exception as exc:
            logger.exception("unexpected StreamChunks error")
            await context.abort(
                grpc.StatusCode.INTERNAL,
                f"unexpected stream error: {type(exc).__name__}",
            )


async def create_kafka_producer() -> AIOKafkaProducer:
    bootstrap_servers = (
        os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        or "ecg-kafka-kafka-bootstrap.ecg-kafka.svc.cluster.local:9092"
    )

    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL") or "PLAINTEXT"

    producer_kwargs = {
        "bootstrap_servers": bootstrap_servers,
        "security_protocol": security_protocol,
        "client_id": os.getenv("KAFKA_CLIENT_ID") or "grpc-stream-adapter",
        "acks": "all",
        "enable_idempotence": True,
    }

    if security_protocol.upper() in ("SSL", "SASL_SSL"):
        import ssl

        cafile = os.getenv("KAFKA_SSL_CAFILE")
        certfile = os.getenv("KAFKA_SSL_CERTFILE")
        keyfile = os.getenv("KAFKA_SSL_KEYFILE")

        ssl_context = ssl.create_default_context(cafile=cafile)

        if certfile and keyfile:
            ssl_context.load_cert_chain(
                certfile=certfile,
                keyfile=keyfile,
            )

        producer_kwargs["ssl_context"] = ssl_context

    producer = AIOKafkaProducer(**producer_kwargs)
    await producer.start()

    logger.info("kafka producer started bootstrap_servers=%s", bootstrap_servers)
    return producer


async def serve():
    grpc_port = int(os.getenv("GRPC_PORT") or "50051")
    kafka_topic = os.getenv("KAFKA_TOPIC") or "ecg.raw"
    kafka_dead_letter_topic = os.getenv("KAFKA_DEAD_LETTER_TOPIC") or "ecg.dead-letter"

    producer = await create_kafka_producer()

    server = grpc.aio.server()

    ecg_ingestion_pb2_grpc.add_EcgIngestionServicer_to_server(
        EcgIngestionService(
            producer=producer,
            topic=kafka_topic,
            dead_letter_topic=kafka_dead_letter_topic,
        ),
        server,
    )

    listen_address = f"[::]:{grpc_port}"
    server.add_insecure_port(listen_address)

    stop_event = asyncio.Event()

    def handle_shutdown():
        logger.info("shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()

    for shutdown_signal in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(shutdown_signal, handle_shutdown)
        except NotImplementedError:
            logger.warning("signal handlers are not supported")

    try:
        await server.start()
        logger.info("grpc server started address=%s", listen_address)

        await stop_event.wait()

    finally:
        logger.info("stopping grpc server")
        await server.stop(grace=10)

        logger.info("stopping kafka producer")
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(serve())
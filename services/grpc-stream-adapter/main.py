import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone

import asyncpg
import grpc
from aiokafka import AIOKafkaProducer

import ecg_ingestion_pb2
import ecg_ingestion_pb2_grpc


logging.basicConfig(
    level=(os.getenv("LOG_LEVEL") or "INFO").upper(),
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


def build_rejected_event(
    chunk: ecg_ingestion_pb2.EcgChunk,
    reason: str,
    mode: str,
) -> dict:
    return {
        "event_type": "ecg.chunk.rejected",
        "ingestion_mode": mode,
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
    mode: str,
) -> dict:
    return {
        "event_type": "ecg.chunk.publish_failed",
        "ingestion_mode": mode,
        "schema_version": chunk.schema_version,
        "source_id": chunk.source_id,
        "session_id": chunk.session_id,
        "sequence_from": chunk.sequence_from,
        "sequence_to": chunk.sequence_to,
        "reason": reason,
        "received_at": utc_now_iso(),
        "received_by": os.getenv("POD_NAME") or "unknown-pod",
    }


async def create_kafka_producer() -> AIOKafkaProducer:
    bootstrap_servers = (
        os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        or "ecg-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
    )

    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL") or "PLAINTEXT"

    producer_kwargs = {
        "bootstrap_servers": bootstrap_servers,
        "security_protocol": security_protocol,
        "client_id": os.getenv("KAFKA_CLIENT_ID") or "grpc-stream-adapter",
        "acks": "all",
        "enable_idempotence": True,
    }

    max_attempts = int(os.getenv("KAFKA_CONNECT_MAX_ATTEMPTS") or "12")
    retry_delay_seconds = float(os.getenv("KAFKA_CONNECT_RETRY_DELAY_SECONDS") or "5")

    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        producer = AIOKafkaProducer(**producer_kwargs)

        try:
            logger.info(
                "connecting to kafka attempt=%s/%s bootstrap_servers=%s",
                attempt,
                max_attempts,
                bootstrap_servers,
            )

            await producer.start()

            logger.info("kafka producer started bootstrap_servers=%s", bootstrap_servers)
            return producer

        except Exception as exc:
            last_error = exc

            logger.warning(
                "failed to start kafka producer attempt=%s/%s error=%s",
                attempt,
                max_attempts,
                type(exc).__name__,
            )

            try:
                await producer.stop()
            except Exception:
                logger.exception("failed to stop kafka producer after failed start")

            if attempt < max_attempts:
                await asyncio.sleep(retry_delay_seconds)

    raise RuntimeError(
        f"unable to connect to kafka after {max_attempts} attempts"
    ) from last_error


async def create_db_pool() -> asyncpg.Pool:
    host = os.getenv("POSTGRES_HOST") or "ecg-postgres-pooler-rw.database.svc.cluster.local"
    port = int(os.getenv("POSTGRES_PORT") or "5432")
    database = os.getenv("POSTGRES_DB") or "ecg"
    user = os.getenv("POSTGRES_USER") or "ecg_app"
    password = os.getenv("POSTGRES_PASSWORD") or "REPLACE_ME"

    pool = await asyncpg.create_pool(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        min_size=1,
        max_size=5,
        command_timeout=30,
        statement_cache_size=0,
    )

    logger.info(
        "postgres pool created host=%s port=%s database=%s user=%s",
        host,
        port,
        database,
        user,
    )

    return pool


async def ensure_session(
    connection: asyncpg.Connection,
    chunk: ecg_ingestion_pb2.EcgChunk,
    timestamp_from: datetime,
    timestamp_to: datetime,
    ingestion_mode: str,
) -> None:
    await connection.execute(
        """
        insert into ecg_sessions (
            session_id,
            source_id,
            ingestion_mode,
            started_at,
            ended_at,
            sampling_rate,
            lead_id,
            status
        )
        values ($1, $2, $3, $4, $5, $6, $7, 'active')
        on conflict (session_id) do update set
            source_id = excluded.source_id,
            ingestion_mode = excluded.ingestion_mode,
            started_at = least(ecg_sessions.started_at, excluded.started_at),
            ended_at = greatest(ecg_sessions.ended_at, excluded.ended_at),
            sampling_rate = excluded.sampling_rate,
            lead_id = excluded.lead_id,
            updated_at = now()
        """,
        chunk.session_id,
        chunk.source_id,
        ingestion_mode,
        timestamp_from,
        timestamp_to,
        chunk.sampling_rate,
        chunk.lead_id,
    )


async def detect_gap(
    connection: asyncpg.Connection,
    chunk: ecg_ingestion_pb2.EcgChunk,
) -> None:
    previous_sequence_to = await connection.fetchval(
        """
        select max(sequence_to)
        from ecg_chunks
        where session_id = $1
          and sequence_to < $2
        """,
        chunk.session_id,
        chunk.sequence_from,
    )

    if previous_sequence_to is None:
        return

    expected_next = previous_sequence_to + 1

    if chunk.sequence_from <= expected_next:
        return

    missing_from = expected_next
    missing_to = chunk.sequence_from - 1
    missing_samples = missing_to - missing_from + 1
    duration_ms = int((missing_samples / chunk.sampling_rate) * 1000)

    await connection.execute(
        """
        insert into ecg_gaps (
            session_id,
            missing_from,
            missing_to,
            duration_ms,
            recovery_status
        )
        values ($1, $2, $3, $4, 'missing')
        on conflict (session_id, missing_from, missing_to) do nothing
        """,
        chunk.session_id,
        missing_from,
        missing_to,
        duration_ms,
    )

    logger.warning(
        "gap detected session_id=%s missing_from=%s missing_to=%s duration_ms=%s",
        chunk.session_id,
        missing_from,
        missing_to,
        duration_ms,
    )


async def store_chunk_direct(
    pool: asyncpg.Pool,
    chunk: ecg_ingestion_pb2.EcgChunk,
) -> bool:
    timestamp_from = parse_iso_datetime(chunk.timestamp_from)
    timestamp_to = parse_iso_datetime(chunk.timestamp_to)

    ingestion_mode = "grpc-direct"
    samples_json = json.dumps(list(chunk.samples), separators=(",", ":"))
    pod_name = os.getenv("POD_NAME") or "unknown-pod"

    async with pool.acquire() as connection:
        async with connection.transaction():
            await ensure_session(
                connection=connection,
                chunk=chunk,
                timestamp_from=timestamp_from,
                timestamp_to=timestamp_to,
                ingestion_mode=ingestion_mode,
            )

            await detect_gap(
                connection=connection,
                chunk=chunk,
            )

            inserted = await connection.fetchrow(
                """
                insert into ecg_chunks (
                    session_id,
                    source_id,
                    ingestion_mode,
                    sequence_from,
                    sequence_to,
                    timestamp_from,
                    timestamp_to,
                    sampling_rate,
                    lead_id,
                    samples_json,
                    kafka_topic,
                    kafka_partition,
                    kafka_offset,
                    received_by_pod,
                    status
                )
                values (
                    $1, $2, $3,
                    $4, $5,
                    $6, $7,
                    $8, $9,
                    $10::jsonb,
                    $11, $12, $13,
                    $14,
                    'stored'
                )
                on conflict (session_id, sequence_from, sequence_to) do nothing
                returning chunk_id
                """,
                chunk.session_id,
                chunk.source_id,
                ingestion_mode,
                chunk.sequence_from,
                chunk.sequence_to,
                timestamp_from,
                timestamp_to,
                chunk.sampling_rate,
                chunk.lead_id,
                samples_json,
                "direct-grpc",
                0,
                0,
                pod_name,
            )

            if inserted is None:
                logger.info(
                    "duplicate direct chunk skipped session_id=%s sequence_from=%s sequence_to=%s",
                    chunk.session_id,
                    chunk.sequence_from,
                    chunk.sequence_to,
                )
                return False

            await connection.execute(
                """
                update ecg_sessions
                set
                    chunks_count = chunks_count + 1,
                    ended_at = greatest(ended_at, $2),
                    updated_at = now()
                where session_id = $1
                """,
                chunk.session_id,
                timestamp_to,
            )

            return True


class EcgIngestionService(ecg_ingestion_pb2_grpc.EcgIngestionServicer):
    def __init__(
        self,
        ingestion_mode: str,
        producer: AIOKafkaProducer | None,
        db_pool: asyncpg.Pool | None,
        kafka_topic: str,
        dead_letter_topic: str,
    ):
        self.ingestion_mode = ingestion_mode
        self.producer = producer
        self.db_pool = db_pool
        self.kafka_topic = kafka_topic
        self.dead_letter_topic = dead_letter_topic

    async def publish_dead_letter(
        self,
        chunk: ecg_ingestion_pb2.EcgChunk,
        event: dict,
    ) -> None:
        if self.producer is None:
            logger.warning(
                "dead-letter skipped because kafka producer is not configured event_type=%s session_id=%s",
                event.get("event_type"),
                chunk.session_id,
            )
            return

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

    async def handle_kafka_chunk(
        self,
        chunk: ecg_ingestion_pb2.EcgChunk,
    ) -> ecg_ingestion_pb2.EcgAck:
        if self.producer is None:
            return build_ack(
                chunk=chunk,
                accepted=False,
                message="kafka producer is not configured",
            )

        try:
            await self.producer.send_and_wait(
                self.kafka_topic,
                key=chunk.session_id.encode("utf-8"),
                value=chunk.SerializeToString(),
                headers=[
                    ("content-type", b"application/x-protobuf"),
                    ("message-type", b"EcgChunk"),
                    ("schema-version", chunk.schema_version.encode("utf-8")),
                    ("ingestion-mode", b"grpc"),
                ],
            )

            logger.info(
                "accepted kafka chunk session_id=%s source_id=%s sequence_from=%s sequence_to=%s topic=%s",
                chunk.session_id,
                chunk.source_id,
                chunk.sequence_from,
                chunk.sequence_to,
                self.kafka_topic,
            )

            return build_ack(
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

            publish_failed_event = build_publish_failed_event(
                chunk=chunk,
                reason=reason,
                mode=self.ingestion_mode,
            )

            try:
                await self.publish_dead_letter(chunk, publish_failed_event)
            except Exception:
                logger.exception(
                    "failed to publish kafka failure event to dead-letter topic session_id=%s",
                    chunk.session_id,
                )

            return build_ack(
                chunk=chunk,
                accepted=False,
                message=reason,
            )

    async def handle_direct_chunk(
        self,
        chunk: ecg_ingestion_pb2.EcgChunk,
    ) -> ecg_ingestion_pb2.EcgAck:
        if self.db_pool is None:
            return build_ack(
                chunk=chunk,
                accepted=False,
                message="postgres pool is not configured",
            )

        try:
            inserted = await store_chunk_direct(
                pool=self.db_pool,
                chunk=chunk,
            )

            logger.info(
                "stored direct chunk session_id=%s source_id=%s sequence_from=%s sequence_to=%s inserted=%s",
                chunk.session_id,
                chunk.source_id,
                chunk.sequence_from,
                chunk.sequence_to,
                inserted,
            )

            if inserted:
                message = "stored in postgres"
            else:
                message = "duplicate skipped"

            return build_ack(
                chunk=chunk,
                accepted=True,
                message=message,
            )

        except Exception as exc:
            logger.exception(
                "failed to store direct chunk session_id=%s sequence_from=%s sequence_to=%s",
                chunk.session_id,
                chunk.sequence_from,
                chunk.sequence_to,
            )

            return build_ack(
                chunk=chunk,
                accepted=False,
                message=f"postgres write failed: {type(exc).__name__}",
            )

    async def StreamChunks(self, request_iterator, context):
        try:
            async for chunk in request_iterator:
                accepted, message = validate_chunk(chunk)

                if not accepted:
                    rejected_event = build_rejected_event(
                        chunk=chunk,
                        reason=message,
                        mode=self.ingestion_mode,
                    )

                    try:
                        await self.publish_dead_letter(chunk, rejected_event)
                    except Exception:
                        logger.exception(
                            "failed to publish rejected chunk to dead-letter topic session_id=%s",
                            chunk.session_id,
                        )

                    logger.warning(
                        "rejected chunk mode=%s session_id=%s sequence_from=%s sequence_to=%s reason=%s",
                        self.ingestion_mode,
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

                    continue

                if self.ingestion_mode == "kafka":
                    yield await self.handle_kafka_chunk(chunk)

                elif self.ingestion_mode == "direct":
                    yield await self.handle_direct_chunk(chunk)

                else:
                    yield build_ack(
                        chunk=chunk,
                        accepted=False,
                        message=f"unsupported INGESTION_MODE={self.ingestion_mode}",
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


async def serve() -> None:
    grpc_port = int(os.getenv("GRPC_PORT") or "50051")
    ingestion_mode = (os.getenv("INGESTION_MODE") or "kafka").lower()

    if ingestion_mode not in ("kafka", "direct"):
        raise ValueError("INGESTION_MODE must be either 'kafka' or 'direct'")

    kafka_topic = os.getenv("KAFKA_TOPIC") or "ecg.raw"
    kafka_dead_letter_topic = os.getenv("KAFKA_DEAD_LETTER_TOPIC") or "ecg.dead-letter"

    producer: AIOKafkaProducer | None = None
    db_pool: asyncpg.Pool | None = None

    if ingestion_mode == "kafka":
        producer = await create_kafka_producer()

    if ingestion_mode == "direct":
        db_pool = await create_db_pool()

    server = grpc.aio.server()

    ecg_ingestion_pb2_grpc.add_EcgIngestionServicer_to_server(
        EcgIngestionService(
            ingestion_mode=ingestion_mode,
            producer=producer,
            db_pool=db_pool,
            kafka_topic=kafka_topic,
            dead_letter_topic=kafka_dead_letter_topic,
        ),
        server,
    )

    listen_address = f"[::]:{grpc_port}"
    server.add_insecure_port(listen_address)

    stop_event = asyncio.Event()

    def handle_shutdown() -> None:
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
        logger.info(
            "grpc ingestion adapter started address=%s mode=%s",
            listen_address,
            ingestion_mode,
        )

        await stop_event.wait()

    finally:
        logger.info("stopping grpc server")
        await server.stop(grace=10)

        if producer is not None:
            logger.info("stopping kafka producer")
            await producer.stop()

        if db_pool is not None:
            logger.info("closing postgres pool")
            await db_pool.close()


if __name__ == "__main__":
    asyncio.run(serve())
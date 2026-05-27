import asyncio
import json
import logging
import os
import signal
from datetime import datetime

import asyncpg
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

import ecg_ingestion_pb2


logging.basicConfig(
    level=(os.getenv("LOG_LEVEL") or "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger("ecg-storage-consumer")


def parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def get_header(headers: list[tuple[str, bytes]], name: str, default: str) -> str:
    for key, value in headers or []:
        if key == name:
            return value.decode("utf-8")
    return default


async def create_db_pool() -> asyncpg.Pool:
    host = os.getenv("POSTGRES_HOST") or "ecg-postgres-pooler-rw.ecg-postgres.svc.cluster.local"
    port = int(os.getenv("POSTGRES_PORT") or "5432")
    database = os.getenv("POSTGRES_DB") or "ecg"
    user = os.getenv("POSTGRES_USER") or "ecg_app"
    password = os.getenv("POSTGRES_PASSWORD") or "ecg_app_password"

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

    logger.info("postgres pool created host=%s port=%s database=%s user=%s", host, port, database, user)
    return pool


async def create_kafka_consumer() -> AIOKafkaConsumer:
    bootstrap_servers = (
        os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        or "ecg-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
    )

    topic = os.getenv("KAFKA_TOPIC") or "ecg.raw"
    group_id = os.getenv("KAFKA_GROUP_ID") or "ecg-storage-consumer"
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL") or "PLAINTEXT"
    auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET") or "earliest"

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        security_protocol=security_protocol,
        enable_auto_commit=False,
        auto_offset_reset=auto_offset_reset,
        client_id=os.getenv("POD_NAME") or "ecg-storage-consumer",
    )

    await consumer.start()

    logger.info(
        "kafka consumer started bootstrap_servers=%s topic=%s group_id=%s",
        bootstrap_servers,
        topic,
        group_id,
    )

    return consumer


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


async def store_chunk(
    pool: asyncpg.Pool,
    chunk: ecg_ingestion_pb2.EcgChunk,
    kafka_topic: str,
    kafka_partition: int,
    kafka_offset: int,
    ingestion_mode: str,
) -> bool:
    timestamp_from = parse_iso_datetime(chunk.timestamp_from)
    timestamp_to = parse_iso_datetime(chunk.timestamp_to)

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
                kafka_topic,
                kafka_partition,
                kafka_offset,
                pod_name,
            )

            if inserted is None:
                logger.info(
                    "duplicate chunk skipped session_id=%s sequence_from=%s sequence_to=%s",
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


async def process_message(pool: asyncpg.Pool, message) -> None:
    chunk = ecg_ingestion_pb2.EcgChunk()
    chunk.ParseFromString(message.value)

    ingestion_mode = get_header(message.headers, "ingestion-mode", "grpc")

    inserted = await store_chunk(
        pool=pool,
        chunk=chunk,
        kafka_topic=message.topic,
        kafka_partition=message.partition,
        kafka_offset=message.offset,
        ingestion_mode=ingestion_mode,
    )

    logger.info(
        "processed chunk session_id=%s sequence_from=%s sequence_to=%s partition=%s offset=%s inserted=%s",
        chunk.session_id,
        chunk.sequence_from,
        chunk.sequence_to,
        message.partition,
        message.offset,
        inserted,
    )


async def run() -> None:
    pool = await create_db_pool()
    consumer = await create_kafka_consumer()

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
        while not stop_event.is_set():
            try:
                message = await asyncio.wait_for(consumer.getone(), timeout=1.0)

                await process_message(pool, message)

                topic_partition = TopicPartition(message.topic, message.partition)
                await consumer.commit(
                    {
                        topic_partition: message.offset + 1,
                    }
                )

            except asyncio.TimeoutError:
                continue

    finally:
        logger.info("stopping kafka consumer")
        await consumer.stop()

        logger.info("closing postgres pool")
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run())
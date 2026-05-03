import asyncio
import logging
import os
import signal
from typing import Any

import aiohttp
import asyncpg
from aiokafka import AIOKafkaProducer

import ecg_ingestion_pb2


logging.basicConfig(
    level=(os.getenv("LOG_LEVEL") or "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger("longpoll-puller")


def to_ecg_chunk(payload: dict[str, Any]) -> ecg_ingestion_pb2.EcgChunk:
    return ecg_ingestion_pb2.EcgChunk(
        schema_version=payload["schema_version"],
        source_id=payload["source_id"],
        session_id=payload["session_id"],
        sequence_from=int(payload["sequence_from"]),
        sequence_to=int(payload["sequence_to"]),
        timestamp_from=payload["timestamp_from"],
        timestamp_to=payload["timestamp_to"],
        sampling_rate=int(payload["sampling_rate"]),
        lead_id=payload["lead_id"],
        samples=[float(value) for value in payload["samples"]],
    )


async def create_kafka_producer() -> AIOKafkaProducer:
    bootstrap_servers = (
        os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        or "ecg-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
    )

    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL") or "PLAINTEXT"

    producer_kwargs = {
        "bootstrap_servers": bootstrap_servers,
        "security_protocol": security_protocol,
        "client_id": os.getenv("KAFKA_CLIENT_ID") or "longpoll-puller",
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
        max_size=3,
        command_timeout=30,
        statement_cache_size=0,
    )

    logger.info("postgres pool created host=%s port=%s database=%s user=%s", host, port, database, user)
    return pool


async def ensure_state_table(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as connection:
        await connection.execute(
            """
            create table if not exists ecg_pull_state (
                source_id text not null,
                session_id text not null,
                last_sequence_to bigint not null default -1,
                updated_at timestamptz not null default now(),
                primary key (source_id, session_id)
            )
            """
        )


async def get_last_sequence(
    pool: asyncpg.Pool,
    source_id: str,
    session_id: str,
) -> int:
    async with pool.acquire() as connection:
        value = await connection.fetchval(
            """
            select last_sequence_to
            from ecg_pull_state
            where source_id = $1
              and session_id = $2
            """,
            source_id,
            session_id,
        )

    if value is None:
        return int(os.getenv("START_AFTER_SEQUENCE") or "-1")

    return int(value)


async def update_last_sequence(
    pool: asyncpg.Pool,
    source_id: str,
    session_id: str,
    last_sequence_to: int,
) -> None:
    async with pool.acquire() as connection:
        await connection.execute(
            """
            insert into ecg_pull_state (
                source_id,
                session_id,
                last_sequence_to,
                updated_at
            )
            values ($1, $2, $3, now())
            on conflict (source_id, session_id) do update set
                last_sequence_to = greatest(ecg_pull_state.last_sequence_to, excluded.last_sequence_to),
                updated_at = now()
            """,
            source_id,
            session_id,
            last_sequence_to,
        )


async def fetch_events(
    http_session: aiohttp.ClientSession,
    source_url: str,
    session_id: str,
    after_sequence: int,
    limit: int,
    wait_timeout_seconds: float,
) -> dict[str, Any]:
    params = {
        "session_id": session_id,
        "after_sequence": str(after_sequence),
        "limit": str(limit),
        "wait_timeout_seconds": str(wait_timeout_seconds),
    }

    async with http_session.get(source_url, params=params) as response:
        response.raise_for_status()
        return await response.json()


async def publish_chunk(
    producer: AIOKafkaProducer,
    topic: str,
    chunk: ecg_ingestion_pb2.EcgChunk,
) -> None:
    await producer.send_and_wait(
        topic,
        key=chunk.session_id.encode("utf-8"),
        value=chunk.SerializeToString(),
        headers=[
            ("content-type", b"application/x-protobuf"),
            ("message-type", b"EcgChunk"),
            ("schema-version", chunk.schema_version.encode("utf-8")),
            ("ingestion-mode", b"longpoll"),
        ],
    )


async def run() -> None:
    source_url = (
        os.getenv("SOURCE_URL")
        or "http://mock-external-buffer-server.ecg-system.svc.cluster.local:8080/api/v1/ecg/events"
    )

    source_id = os.getenv("SOURCE_ID") or "mock-external-buffer-server-001"
    session_id = os.getenv("SESSION_ID") or "longpoll-session-001"
    kafka_topic = os.getenv("KAFKA_TOPIC") or "ecg.raw"

    batch_limit = int(os.getenv("BATCH_LIMIT") or "10")
    wait_timeout_seconds = float(os.getenv("WAIT_TIMEOUT_SECONDS") or "10")
    idle_sleep_seconds = float(os.getenv("IDLE_SLEEP_SECONDS") or "1")
    max_empty_polls = int(os.getenv("MAX_EMPTY_POLLS") or "0")

    producer = await create_kafka_producer()
    pool = await create_db_pool()
    await ensure_state_table(pool)

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

    empty_polls = 0

    try:
        async with aiohttp.ClientSession() as http_session:
            while not stop_event.is_set():
                after_sequence = await get_last_sequence(
                    pool=pool,
                    source_id=source_id,
                    session_id=session_id,
                )

                logger.info(
                    "polling source_url=%s session_id=%s after_sequence=%s",
                    source_url,
                    session_id,
                    after_sequence,
                )

                try:
                    response = await fetch_events(
                        http_session=http_session,
                        source_url=source_url,
                        session_id=session_id,
                        after_sequence=after_sequence,
                        limit=batch_limit,
                        wait_timeout_seconds=wait_timeout_seconds,
                    )

                except Exception:
                    logger.exception("failed to fetch events from external buffer server")
                    await asyncio.sleep(idle_sleep_seconds)
                    continue

                chunks = response.get("chunks", [])

                if not chunks:
                    empty_polls += 1

                    logger.info(
                        "no new chunks session_id=%s empty_polls=%s",
                        session_id,
                        empty_polls,
                    )

                    if max_empty_polls > 0 and empty_polls >= max_empty_polls:
                        logger.info("max empty polls reached, stopping")
                        break

                    await asyncio.sleep(idle_sleep_seconds)
                    continue

                empty_polls = 0

                for chunk_payload in chunks:
                    chunk = to_ecg_chunk(chunk_payload)

                    await publish_chunk(
                        producer=producer,
                        topic=kafka_topic,
                        chunk=chunk,
                    )

                    await update_last_sequence(
                        pool=pool,
                        source_id=source_id,
                        session_id=session_id,
                        last_sequence_to=chunk.sequence_to,
                    )

                    logger.info(
                        "published longpoll chunk session_id=%s sequence_from=%s sequence_to=%s topic=%s",
                        chunk.session_id,
                        chunk.sequence_from,
                        chunk.sequence_to,
                        kafka_topic,
                    )

    finally:
        logger.info("stopping kafka producer")
        await producer.stop()

        logger.info("closing postgres pool")
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run())
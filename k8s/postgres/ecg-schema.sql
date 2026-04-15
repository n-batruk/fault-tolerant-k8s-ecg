create table if not exists ecg_sessions (
    session_id text primary key,
    source_id text not null,
    ingestion_mode text not null default 'grpc',
    started_at timestamptz not null,
    ended_at timestamptz not null,
    sampling_rate integer not null,
    lead_id text not null,
    chunks_count integer not null default 0,
    status text not null default 'active',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists ecg_chunks (
    chunk_id bigserial primary key,
    session_id text not null references ecg_sessions(session_id) on delete cascade,
    source_id text not null,
    ingestion_mode text not null default 'grpc',

    sequence_from bigint not null,
    sequence_to bigint not null,

    timestamp_from timestamptz not null,
    timestamp_to timestamptz not null,

    sampling_rate integer not null,
    lead_id text not null,

    -- Move to object storage in the future
    samples_json jsonb not null,

    kafka_topic text not null,
    kafka_partition integer not null,
    kafka_offset bigint not null,

    received_by_pod text not null,
    stored_at timestamptz not null default now(),
    status text not null default 'stored',

    unique (session_id, sequence_from, sequence_to)
);

create index if not exists idx_ecg_chunks_session_sequence
    on ecg_chunks (session_id, sequence_from, sequence_to);

create index if not exists idx_ecg_chunks_kafka_position
    on ecg_chunks (kafka_topic, kafka_partition, kafka_offset);

create table if not exists ecg_gaps (
    gap_id bigserial primary key,
    session_id text not null references ecg_sessions(session_id) on delete cascade,

    missing_from bigint not null,
    missing_to bigint not null,
    duration_ms bigint not null,

    detected_at timestamptz not null default now(),
    recovery_status text not null default 'missing',

    unique (session_id, missing_from, missing_to)
);

create index if not exists idx_ecg_gaps_session
    on ecg_gaps (session_id, missing_from, missing_to);

create table if not exists analysis_results (
    result_id bigserial primary key,
    session_id text not null references ecg_sessions(session_id) on delete cascade,

    window_from bigint not null,
    window_to bigint not null,

    algorithm_name text not null,
    result_json jsonb not null,

    created_at timestamptz not null default now()
);
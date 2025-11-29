-- Стейдж-таблица с данными из CRM (клиенты + протезы)
CREATE TABLE IF NOT EXISTS stg_crm_users
(
    crm_user_id   String,
    prosthesis_id String,
    country       String,
    activated_at  DateTime
)
    ENGINE = MergeTree
    ORDER BY (crm_user_id, prosthesis_id);

-- Стейдж-таблица с телеметрией
CREATE TABLE IF NOT EXISTS stg_telemetry
(
    prosthesis_id     String,
    ts                DateTime,
    response_time_ms  Float64,
    is_error          UInt8
)
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(ts)
    ORDER BY (prosthesis_id, ts);

-- Витрина для сервиса отчётов
CREATE TABLE IF NOT EXISTS mart_user_telemetry
(
    user_id         String,
    day             Date,
    events_count    UInt64,
    avg_response_ms Float64,
    p95_response_ms Float64,
    errors_count    UInt64
)
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(day)
    ORDER BY (user_id, day);

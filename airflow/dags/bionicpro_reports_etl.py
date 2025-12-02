from __future__ import annotations

import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# Настройки ClickHouse — совпадают с docker-compose
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "bionicpro")
CLICKHOUSE_HTTP_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/"


def execute_clickhouse_sql(sql: str) -> None:
    statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
    for stmt in statements:
        response = requests.post(
            CLICKHOUSE_HTTP_URL,
            params={"database": CLICKHOUSE_DB},
            data=stmt.encode("utf-8"),
            timeout=10,
        )
        # на всякий случай логируем текст ошибки
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            print("ClickHouse error for statement:\n", stmt)
            print("Response text:\n", response.text)
            raise exc


def init_clickhouse_schema(**_: dict) -> None:
    """
    Создаёт (если ещё не созданы) таблицы:
      * stg_crm_users
      * stg_telemetry
      * mart_user_telemetry

    DDL прописан прямо здесь, чтобы не зависеть от содержимого .sql-файла.
    """
    ddl_sql = """
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
        prosthesis_id    String,
        ts               DateTime,
        response_time_ms Float64,
        is_error         UInt8
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
    """
    execute_clickhouse_sql(ddl_sql)


def load_staging_from_crm(**_: dict) -> None:
    """
    Имитация шага ETL из CRM.
    """
    sql = """
    TRUNCATE TABLE IF EXISTS stg_crm_users;

    INSERT INTO stg_crm_users (crm_user_id, prosthesis_id, country, activated_at) VALUES
        ('prothetic1', 'prosthesis-1', 'RU', now() - INTERVAL 30 DAY),
        ('prothetic2', 'prosthesis-2', 'RU', now() - INTERVAL 10 DAY);
    """
    execute_clickhouse_sql(sql)


def load_staging_telemetry(**_: dict) -> None:
    """
    Имитация загрузки телеметрии.
    """
    sql = """
    TRUNCATE TABLE IF EXISTS stg_telemetry;

    INSERT INTO stg_telemetry (prosthesis_id, ts, response_time_ms, is_error) VALUES
        ('prosthesis-1', now() - INTERVAL 3 DAY,  80.0, 0),
        ('prosthesis-1', now() - INTERVAL 2 DAY,  95.0, 0),
        ('prosthesis-1', now() - INTERVAL 1 DAY, 120.0, 1),
        ('prosthesis-2', now() - INTERVAL 3 DAY,  90.0, 0),
        ('prosthesis-2', now() - INTERVAL 2 DAY, 100.0, 0),
        ('prosthesis-2', now() - INTERVAL 1 DAY, 110.0, 1);
    """
    execute_clickhouse_sql(sql)


def build_user_telemetry_mart(**_: dict) -> None:
    """
    Строит витрину mart_user_telemetry из stg_crm_users и stg_telemetry.
    Перед загрузкой витрина очищается, чтобы на каждый запуск пересобираться целиком.
    """
    sql = """
    TRUNCATE TABLE IF EXISTS mart_user_telemetry;

    INSERT INTO mart_user_telemetry (user_id, day, events_count, avg_response_ms, p95_response_ms, errors_count)
    SELECT
        cu.crm_user_id                              AS user_id,
        toDate(t.ts)                                AS day,
        count()                                     AS events_count,
        avg(t.response_time_ms)                     AS avg_response_ms,
        quantile(0.95)(t.response_time_ms)          AS p95_response_ms,
        sumIf(1, t.is_error = 1)                    AS errors_count
    FROM stg_telemetry AS t
    INNER JOIN stg_crm_users AS cu
        ON t.prosthesis_id = cu.prosthesis_id
    GROUP BY
        user_id,
        day
    ORDER BY
        user_id,
        day;
    """
    execute_clickhouse_sql(sql)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bionicpro_reports_etl",
    description="ETL для витрины отчётов BionicPRO: CRM + телеметрия в ClickHouse (mart_user_telemetry).",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",  # раз в час
    catchup=False,
    tags=["bionicpro", "reports", "etl"],
) as dag:
    init_schema = PythonOperator(
        task_id="init_clickhouse_schema",
        python_callable=init_clickhouse_schema,
    )

    load_crm = PythonOperator(
        task_id="load_staging_from_crm",
        python_callable=load_staging_from_crm,
    )

    load_telemetry = PythonOperator(
        task_id="load_staging_telemetry",
        python_callable=load_staging_telemetry,
    )

    build_mart = PythonOperator(
        task_id="build_user_telemetry_mart",
        python_callable=build_user_telemetry_mart,
    )

    init_schema >> load_crm >> load_telemetry >> build_mart

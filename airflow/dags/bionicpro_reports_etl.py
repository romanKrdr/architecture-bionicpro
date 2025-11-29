from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Для ClickHouse используем стандартный драйвер.
# Подключение настраивается в Airflow как connection с conn_id = "clickhouse_default".
from clickhouse_driver import Client


def get_clickhouse_client() -> Client:
    """
    Создаёт ClickHouse-клиент.
    Параметры подключения задаются через Airflow Connection (host/port/user/password/database).
    В простом учебном окружении можно захардкодить host='clickhouse'.
    """
    return Client(host="clickhouse", database="bionicpro")


def extract_crm(**context: Any) -> None:
    """Извлекает данные о клиентах и их протезах из CRM (PostgreSQL) в стейдж-таблицу ClickHouse."""
    pg_hook = PostgresHook(postgres_conn_id="crm_postgres")
    ch_client = get_clickhouse_client()

    sql_crm = """
        SELECT
            c.id           AS crm_user_id,
            p.id           AS prosthesis_id,
            c.country      AS country,
            p.activated_at AS activated_at
        FROM crm_prostheses p
        JOIN crm_clients   c ON c.id = p.client_id
        WHERE p.activated_at IS NOT NULL
    """

    rows = pg_hook.get_records(sql_crm)

    # Структура stg_crm_users:
    # (crm_user_id String, prosthesis_id String, country String, activated_at DateTime)
    if rows:
        ch_client.execute(
            """
            INSERT INTO stg_crm_users (crm_user_id, prosthesis_id, country, activated_at)
            VALUES
            """,
            rows,
        )


def extract_telemetry(**context: Any) -> None:
    """Извлекает телеметрию из базы BionicPRO (PostgreSQL) в стейдж-таблицу ClickHouse."""
    pg_hook = PostgresHook(postgres_conn_id="telemetry_postgres")
    ch_client = get_clickhouse_client()

    execution_date = context["data_interval_start"]
    next_execution_date = context["data_interval_end"]

    sql_telemetry = """
        SELECT
            prosthesis_id,
            ts,
            response_time_ms,
            is_error
        FROM telemetry_events
        WHERE ts >= %(from)s AND ts < %(to)s
    """

    rows = pg_hook.get_records(
        sql_telemetry,
        parameters={"from": execution_date, "to": next_execution_date},
    )

    # Структура stg_telemetry:
    # (prosthesis_id String, ts DateTime, response_time_ms Float64, is_error UInt8)
    if rows:
        ch_client.execute(
            """
            INSERT INTO stg_telemetry (prosthesis_id, ts, response_time_ms, is_error)
            VALUES
            """,
            rows,
        )


def build_mart(**context: Any) -> None:
    """Строит витрину mart_user_telemetry в ClickHouse на основе стейдж-таблиц."""
    ch_client = get_clickhouse_client()

    execution_date = context["data_interval_start"]
    next_execution_date = context["data_interval_end"]

    # Итоговая витрина mart_user_telemetry:
    # user_id String,
    # day Date,
    # events_count UInt64,
    # avg_response_ms Float64,
    # p95_response_ms Float64,
    # errors_count UInt64
    ch_client.execute(
        """
        INSERT INTO mart_user_telemetry
        SELECT
            c.crm_user_id AS user_id,
            toDate(t.ts)  AS day,
            count()       AS events_count,
            avg(t.response_time_ms)              AS avg_response_ms,
            quantile(0.95)(t.response_time_ms)   AS p95_response_ms,
            sumIf(1, t.is_error = 1)             AS errors_count
        FROM stg_telemetry t
        JOIN stg_crm_users c
          ON t.prosthesis_id = c.prosthesis_id
        WHERE t.ts >= %(from)s AND t.ts < %(to)s
        GROUP BY user_id, day
        """,
        {"from": execution_date, "to": next_execution_date},
    )


def cleanup_staging(**context: Any) -> None:
    """Опционально очищаем стейдж-таблицы после построения витрины."""
    ch_client = get_clickhouse_client()
    ch_client.execute("TRUNCATE TABLE stg_crm_users")
    ch_client.execute("TRUNCATE TABLE stg_telemetry")


default_args = {
    "owner": "bionicpro",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bionicpro_reports_etl",
    description="ETL CRM + Telemetry -> ClickHouse mart_user_telemetry",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 * * * *",  # раз в час
    catchup=False,
    tags=["bionicpro", "reports"],
) as dag:

    t_extract_crm = PythonOperator(
        task_id="extract_crm",
        python_callable=extract_crm,
        provide_context=True,
    )

    t_extract_telemetry = PythonOperator(
        task_id="extract_telemetry",
        python_callable=extract_telemetry,
        provide_context=True,
    )

    t_build_mart = PythonOperator(
        task_id="build_mart",
        python_callable=build_mart,
        provide_context=True,
    )

    t_cleanup = PythonOperator(
        task_id="cleanup_staging",
        python_callable=cleanup_staging,
        provide_context=True,
    )

    [t_extract_crm, t_extract_telemetry] >> t_build_mart >> t_cleanup
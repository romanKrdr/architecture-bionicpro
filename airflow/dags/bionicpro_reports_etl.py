from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bionicpro_reports_etl",
    description=(
        "Учебный ETL для витрины отчётов BionicPRO: "
        "агрегация телеметрии и CRM в OLAP (ClickHouse)."
    ),
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",  # раз в час
    catchup=False,
    tags=["bionicpro", "reports", "etl"],
) as dag:
    start = EmptyOperator(task_id="start")
    build_mart = EmptyOperator(task_id="build_user_telemetry_mart")
    finish = EmptyOperator(task_id="finish")

    start >> build_mart >> finish
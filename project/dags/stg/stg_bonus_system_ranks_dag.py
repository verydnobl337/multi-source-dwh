from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import psycopg2

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "load_ranks_table",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)


def load_ranks():
    source_conn_config = BaseHook.get_connection("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
    dest_conn_config = BaseHook.get_connection("PG_WAREHOUSE_CONNECTION")

    source_conn = psycopg2.connect(
        host=source_conn_config.host,
        port=source_conn_config.port,
        dbname=source_conn_config.schema,
        user=source_conn_config.login,
        password=source_conn_config.password,
        sslmode="require",
    )

    dest_conn = psycopg2.connect(
        host=dest_conn_config.host,
        port=dest_conn_config.port,
        dbname=dest_conn_config.schema,
        user=dest_conn_config.login,
        password=dest_conn_config.password,
        sslmode="disable",
    )

    source_cursor = source_conn.cursor()
    dest_cursor = dest_conn.cursor()

    source_cursor.execute("SELECT * FROM ranks")
    rows = source_cursor.fetchall()
    for row in rows:
        dest_cursor.execute(
            "INSERT INTO stg.bonussystem_ranks VALUES (%s, %s, %s, %s)", row
        )

    dest_conn.commit()

    source_cursor.close()
    dest_cursor.close()
    source_conn.close()
    dest_conn.close()


load_ranks_task = PythonOperator(
    task_id="load_ranks",
    python_callable=load_ranks,
    dag=dag,
)

load_ranks_task
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import psycopg2
import json

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "load_events_table",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)


def events_load():
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

    try:
        source_cursor = source_conn.cursor()
        dest_cursor = dest_conn.cursor()

        wf_key = "events_load_workflow"
        last_loaded_id = get_last_loaded_id(dest_cursor, wf_key)

        source_cursor.execute(
            "SELECT id, event_ts, event_type, event_value "
            "FROM outbox "
            "WHERE id > %s "
            "ORDER BY id ASC",
            (last_loaded_id,),
        )
        # Загружаем только события, которые появились
        # после последнего успешно обработанного ID.
        new_events = source_cursor.fetchall()

        with dest_conn:
            for event in new_events:
                dest_cursor.execute(
                    "INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (id) DO NOTHING",
                    event,
                )
            max_id = max(event[0] for event in new_events)
            save_last_loaded_id(dest_cursor, wf_key, max_id)
    except Exception as e:
        print(f"Error during events loading: {e}")
        raise
    finally:
        if source_cursor:
            source_cursor.close()
        if dest_cursor:
            dest_cursor.close()
        source_conn.close()
        dest_conn.close()


def get_last_loaded_id(cursor, workflow_key):
    cursor.execute(
        "SELECT workflow_settings FROM stg.srv_wf_settings WHERE workflow_key = %s",
        (workflow_key,),
    )
    result = cursor.fetchone()

    if result:
        # Проверяем тип: если уже dict, используем как есть
        if isinstance(result[0], dict):
            settings = result[0]
        else:
            settings = json.loads(result[0])
        return settings.get("last_loaded_id", -1)
    else:
        cursor.execute(
            "INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings) "
            "VALUES (%s, %s)",
            (workflow_key, json.dumps({"last_loaded_id": -1})),
        )
        return -1


def save_last_loaded_id(cursor, workflow_key, last_loaded_id):
    """Сохраняет последний загруженный ID в stg.srv_wf_settings"""
    cursor.execute(
        "UPDATE stg.srv_wf_settings "
        "SET workflow_settings = %s "
        "WHERE workflow_key = %s",
        (json.dumps({"last_loaded_id": last_loaded_id}), workflow_key),
    )


load_events_task = PythonOperator(
    task_id="load_events",
    python_callable=events_load,
    dag=dag,
)

load_events_task

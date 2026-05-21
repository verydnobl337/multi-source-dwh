import json
import logging
import requests

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from lib import ConnectionBuilder

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="stg_api_restaurants_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)
def stg_api_restaurants_dag():

    @task()
    def load_api_restaurants():
        log = logging.getLogger(__name__)
        conn = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

        api_conn = BaseHook.get_connection("API_CONNECTION")
        base_url = api_conn.host.rstrip("/")
        endpoint = f"{base_url}/restaurants"
        api_key = "25c27781-8fde-4b30-a22e-524044a7580f"

        HEADERS = {
            "X-Nickname": "verydnob",
            "X-Cohort": "14",
            "X-API-KEY": api_key,
        }
        limit = 50
        offset = 0

        with conn.connection() as c:
            with c.cursor() as cur:
                while True:
                    params = {
                        "limit": limit,
                        "offset": offset,
                        "sort_field": "id",
                        "sort_direction": "asc",
                    }

                    response = requests.get(endpoint, headers=HEADERS, params=params)

                    if response.status_code != 200:
                        log.error(f"API error: {response.text}")
                        raise Exception("API request failed")

                    data = response.json()

                    if not data:
                        log.info("No data received from API")
                        break

                    for row in data:
                        cur.execute(
                            """
                            INSERT INTO stg.api_restaurants (object_id, object_value, update_ts)
                            VALUES(%s, %s, NOW())
                            ON CONFLICT (object_id)
                            DO UPDATE SET
                                object_value = EXCLUDED.object_value,
                                update_ts = NOW()
                        """,
                            (row["_id"], json.dumps(row, ensure_ascii=False)),
                        )
                    offset += limit
                c.commit()
        log.info("STG restaurants loaded successfully")

    load_api_restaurants()


stg_api_restaurants_dag = stg_api_restaurants_dag()

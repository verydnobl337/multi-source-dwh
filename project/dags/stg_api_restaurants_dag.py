import json
import logging
import requests

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from lib import ConnectionBuilder

API_URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants"

HEADERS = {
    "X-Nickname": "verydnob",
    "X-Cohort": "14",
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
}

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

                    response = requests.get(API_URL, headers=HEADERS, params=params)

                    if response.status_code != 200:
                        log.error(f"API error: {response.text}")
                        break

                    data = response.json()

                    if not data:
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
                            (row["_id"], json.dumps(row)),
                        )
                    offset += limit
            c.commit()
        log.info("STG restaurants loaded successfully")

    load_api_restaurants()


stg_api_restaurants_dag = stg_api_restaurants_dag()

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
    dag_id="stg_api_deliveries_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)
def stg_api_deliveries_dag():

    @task()
    def load_api_deliveries():

        log = logging.getLogger(__name__)
        conn = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

        api_conn = BaseHook.get_connection("API_CONNECTION")

        base_url = api_conn.host.rstrip("/")
        endpoint = f"{base_url}/deliveries"

        api_key = "25c27781-8fde-4b30-a22e-524044a7580f"

        HEADERS = {
            "X-Nickname": "verydnob",
            "X-Cohort": "14",
            "X-API-KEY": api_key,
        }

        limit = 50
        offset = 0

        # окно загрузки — последние 7 дней
        from_date = (datetime.utcnow() - timedelta(days=7)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        to_date = datetime.utcnow().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        with conn.connection() as c:
            with c.cursor() as cur:

                while True:

                    params = {
                        "from": from_date,
                        "to": to_date,
                        "limit": limit,
                        "offset": offset,
                        "sort_field": "date",
                        "sort_direction": "asc",
                    }

                    response = requests.get(
                        endpoint,
                        headers=HEADERS,
                        params=params,
                    )

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
                            INSERT INTO stg.api_deliveries (
                                object_id,
                                object_value,
                                update_ts
                            )
                            VALUES (%s, %s, NOW())

                            ON CONFLICT (object_id)
                            DO UPDATE SET
                                object_value = EXCLUDED.object_value,
                                update_ts = NOW();
                            """,
                            (
                                row["delivery_id"],
                                json.dumps(row, ensure_ascii=False),
                            ),
                        )

                    log.info(
                        f"Loaded {len(data)} deliveries with offset={offset}"
                    )

                    offset += limit

                    if len(data) < limit:
                        break

                c.commit()

        log.info("STG deliveries loaded successfully")

    load_api_deliveries()


stg_api_deliveries_dag = stg_api_deliveries_dag()
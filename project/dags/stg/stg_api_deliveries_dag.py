import json
import logging
import requests

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
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

        api_key = Variable.get("API_KEY")
        api_nickname = Variable.get("API_NICKNAME")
        api_cohort = Variable.get("API_COHORT")
        limit = int(Variable.get("API_PAGE_LIMIT", default_var="50"))
        days_back = int(
            Variable.get("API_DELIVERIES_DAYS_BACK", default_var="7")
        )

        # Формируем заголовки HTTP-запроса.
        headers = {
            "X-Nickname": api_nickname,
            "X-Cohort": api_cohort,
            "X-API-KEY": api_key,
        }

        offset = 0

        from_date = (
            datetime.utcnow() - timedelta(days=days_back)
        ).strftime("%Y-%m-%d %H:%M:%S")

        to_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

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
                        headers=headers,
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
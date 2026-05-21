import json
import logging

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from lib import ConnectionBuilder

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="dds_dm_couriers_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)
def dds_dm_couriers_dag():

    @task()
    def load_dm_couriers():

        log = logging.getLogger(__name__)
        conn = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

        with conn.connection() as c:
            with c.cursor() as cur:

                cur.execute("""
                    SELECT
                        object_id,
                        object_value
                    FROM stg.api_couriers
                """)

                couriers = cur.fetchall()

                for courier in couriers:

                    object_id = courier[0]
                    object_value = json.loads(courier[1])

                    courier_name = object_value["name"]

                    cur.execute(
                        """
                        INSERT INTO dds.dm_couriers (
                            courier_id,
                            courier_name
                        )
                        VALUES (%s, %s)
                        ON CONFLICT (courier_id)
                        DO UPDATE SET
                            courier_name = EXCLUDED.courier_name;
                        """,
                        (
                            object_id,
                            courier_name,
                        ),
                    )

            c.commit()

        log.info("dm_couriers loaded successfully")

    load_dm_couriers()


dds_dm_couriers_dag = dds_dm_couriers_dag()
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
    dag_id="dds_fct_deliveries_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)
def dds_fct_deliveries_dag():

    @task()
    def load_fct_deliveries():

        log = logging.getLogger(__name__)
        conn = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

        with conn.connection() as c:
            with c.cursor() as cur:

                cur.execute("""
                    SELECT object_value
                    FROM stg.api_deliveries
                """)

                rows = cur.fetchall()

                for row in rows:

                    data = json.loads(row[0])

                    order_id = data["order_id"]
                    courier_id = data["courier_id"]
                    delivery_id = data["delivery_id"]
                    order_ts = data["order_ts"]
                    delivery_ts = data["delivery_ts"]
                    rate = data["rate"]
                    tip_sum = data["tip_sum"]

                    cur.execute(
                        """
                        INSERT INTO dds.fct_deliveries (
                            order_id,
                            courier_id,
                            delivery_id,
                            order_ts,
                            delivery_ts,
                            rate,
                            tip_sum
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (delivery_id)
                        DO UPDATE SET
                            order_id = EXCLUDED.order_id,
                            courier_id = EXCLUDED.courier_id,
                            order_ts = EXCLUDED.order_ts,
                            delivery_ts = EXCLUDED.delivery_ts,
                            rate = EXCLUDED.rate,
                            tip_sum = EXCLUDED.tip_sum;
                        """,
                        (
                            order_id,
                            courier_id,
                            delivery_id,
                            order_ts,
                            delivery_ts,
                            rate,
                            tip_sum,
                        ),
                    )

            c.commit()

        log.info("dds.fct_deliveries loaded successfully")

    load_fct_deliveries()


dds_fct_deliveries_dag = dds_fct_deliveries_dag()

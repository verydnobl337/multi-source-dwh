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
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)
def cdm_dm_settlement_report():

    @task()
    def load_dm_settlement_report_to_cdm():

        log = logging.getLogger(__name__)
        conn = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

        insert_query = """
            INSERT INTO cdm.dm_settlement_report (
                restaurant_id,
                restaurant_name,
                settlement_date,
                orders_count,
                orders_total_sum,
                orders_bonus_payment_sum,
                orders_bonus_granted_sum,
                order_processing_fee,
                restaurant_reward_sum
            )
            SELECT
                dr.id as restaurant_id,
                dr.restaurant_name,
                dt.date as settlement_date,
                COUNT(DISTINCT fps.order_id) as orders_count,
                SUM(fps.total_sum) as orders_total_sum,
                SUM(fps.bonus_payment) as orders_bonus_payment_sum,
                SUM(fps.bonus_grant) as orders_bonus_granted_sum,
                SUM(fps.total_sum) * 0.25 as order_processing_fee,
                SUM(fps.total_sum) - SUM(fps.bonus_payment) - (SUM(fps.total_sum) * 0.25) as restaurant_reward_sum
            FROM dds.fct_product_sales fps
            JOIN dds.dm_orders fo ON fps.order_id = fo.id
            JOIN dds.dm_restaurants dr ON fo.restaurant_id = dr.id
            JOIN dds.dm_timestamps dt ON fo.timestamp_id = dt.id
            WHERE fo.order_status = 'CLOSED'
            GROUP BY 
                dr.id,
                dr.restaurant_name,
                dt.date
            ON CONFLICT (restaurant_id, settlement_date)
            DO UPDATE SET
                restaurant_name = EXCLUDED.restaurant_name,
                orders_count = EXCLUDED.orders_count,
                orders_total_sum = EXCLUDED.orders_total_sum,
                orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                order_processing_fee = EXCLUDED.order_processing_fee,
                restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
        """

        with conn.connection() as c:
            with c.cursor() as cur:
                cur.execute(insert_query)
                c.commit()

        log.info("dm_settlement_report loaded")

    load_dm_settlement_report_to_cdm()


cdm_dm_settlement_report_dag = cdm_dm_settlement_report()

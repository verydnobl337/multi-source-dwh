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
    dag_id="cdm_dm_courier_ledger_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=60),
    catchup=False,
)
def cdm_dm_courier_ledger():

    @task()
    def load_courier_ledger():

        log = logging.getLogger(__name__)
        conn = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

        # Формируем итоговую витрину выплат курьерам.
        # Расчёт выполняется по курьеру и месяцу.
        insert_query = """
        -- Собираем исходные данные по доставкам, курьерам,
        -- заказам и продажам перед расчётом агрегатов.
        WITH base AS (
            SELECT
                c.courier_id,
                c.courier_name,
                EXTRACT(YEAR FROM d.order_ts)::int2 AS settlement_year,
                EXTRACT(MONTH FROM d.order_ts)::int2 AS settlement_month,
                d.order_id,
                d.rate,
                d.tip_sum,
                fps.total_sum
            FROM dds.fct_deliveries d
            JOIN dds.dm_couriers c
                ON c.courier_id = d.courier_id
            JOIN dds.dm_orders o
                ON o.order_key = d.order_id
            JOIN dds.fct_product_sales fps
                ON fps.order_id = o.id
        ),
        -- Агрегируем показатели по курьеру за месяц.
        agg AS (
            SELECT
                courier_id,
                courier_name,
                settlement_year,
                settlement_month,
                COUNT(DISTINCT order_id) AS orders_count,
                SUM(total_sum) AS orders_total_sum,
                AVG(rate) AS rate_avg,
                SUM(tip_sum) AS courier_tips_sum
            FROM base
            GROUP BY
                courier_id,
                courier_name,
                settlement_year,
                settlement_month
        ),
        -- Рассчитываем комиссию и коэффициент выплаты
        -- в зависимости от среднего рейтинга курьера.
        calc AS (
            SELECT
                *,
                orders_total_sum * 0.25 AS order_processing_fee,
                CASE
                    WHEN rate_avg < 4 THEN 0.05
                    WHEN rate_avg < 4.5 THEN 0.07
                    WHEN rate_avg < 4.9 THEN 0.08
                    ELSE 0.10
                END AS rate_coef
            FROM agg
        )
        INSERT INTO cdm.dm_courier_ledger (
            courier_id,
            courier_name,
            settlement_year,
            settlement_month,
            orders_count,
            orders_total_sum,
            rate_avg,
            order_processing_fee,
            courier_order_sum,
            courier_tips_sum,
            courier_reward_sum
        )
        SELECT
            courier_id,
            courier_name,
            settlement_year,
            settlement_month,
            orders_count,
            orders_total_sum,
            rate_avg,
            order_processing_fee,

            GREATEST(
                orders_total_sum * rate_coef,
                CASE
                    WHEN rate_avg < 4 THEN 100 * orders_count
                    WHEN rate_avg < 4.5 THEN 150 * orders_count
                    WHEN rate_avg < 4.9 THEN 175 * orders_count
                    ELSE 200 * orders_count
                END
            ) AS courier_order_sum,

            courier_tips_sum,

            (
                GREATEST(
                    orders_total_sum * rate_coef,
                    CASE
                        WHEN rate_avg < 4 THEN 100 * orders_count
                        WHEN rate_avg < 4.5 THEN 150 * orders_count
                        WHEN rate_avg < 4.9 THEN 175 * orders_count
                        ELSE 200 * orders_count
                    END
                )
                + courier_tips_sum * 0.95
            ) AS courier_reward_sum
        FROM calc
        ON CONFLICT (courier_id, settlement_year, settlement_month)
        DO UPDATE SET
            courier_name = EXCLUDED.courier_name,
            orders_count = EXCLUDED.orders_count,
            orders_total_sum = EXCLUDED.orders_total_sum,
            rate_avg = EXCLUDED.rate_avg,
            order_processing_fee = EXCLUDED.order_processing_fee,
            courier_order_sum = EXCLUDED.courier_order_sum,
            courier_tips_sum = EXCLUDED.courier_tips_sum,
            courier_reward_sum = EXCLUDED.courier_reward_sum;
        """

        with conn.connection() as c:
            with c.cursor() as cur:
                cur.execute(insert_query)
                c.commit()

        log.info("CDM courier ledger loaded successfully")

    load_courier_ledger()


cdm_dm_courier_ledger_dag = cdm_dm_courier_ledger()

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from lib import ConnectionBuilder
from lib.dict_util import str2json
import logging

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="dds_fct_product_sales_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)
def dds_fct_product_sales():

    @task()
    def load_product_sales_to_dds():

        log = logging.getLogger(__name__)
        conn = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

        select_query = """
            SELECT event_value
            FROM stg.bonussystem_events
            WHERE event_type = 'bonus_transaction'
        """

        get_order_id = """
            SELECT id
            FROM dds.dm_orders
            WHERE order_key = %(order_key)s
            LIMIT 1
        """

        get_product_id = """
            SELECT id
            FROM dds.dm_products
            WHERE product_id = %(product_id)s
              AND active_from <= %(order_dt)s
              AND active_to > %(order_dt)s
            ORDER BY active_from DESC
            LIMIT 1
        """

        insert_query = """
            INSERT INTO dds.fct_product_sales(
                product_id,
                order_id,
                count,
                price,
                total_sum,
                bonus_payment,
                bonus_grant
            )
            VALUES (
                %(product_id)s,
                %(order_id)s,
                %(count)s,
                %(price)s,
                %(total_sum)s,
                %(bonus_payment)s,
                %(bonus_grant)s
            )
            ON CONFLICT DO NOTHING
        """

        # Получаем события бонусной системы и связываем их
        # с заказами и продуктами DDS.
        with conn.connection() as c:
            with c.cursor() as cur:

                cur.execute(select_query)
                rows = cur.fetchall()

                inserted = 0

                for r in rows:
                    event = str2json(r[0])

                    order_key = event["order_id"]
                    order_dt = event["order_date"]

                    cur.execute(get_order_id, {"order_key": order_key})
                    order_row = cur.fetchone()
                    if not order_row:
                        continue

                    order_id = order_row[0]

                    for p in event.get("product_payments", []):

                        product_origin_id = p["product_id"]

                        cur.execute(
                            get_product_id,
                            {"product_id": product_origin_id, "order_dt": order_dt},
                        )

                        prod_row = cur.fetchone()
                        if not prod_row:
                            continue

                        product_id = prod_row[0]

                        qty = int(p["quantity"])
                        price = float(p["price"])

                        cur.execute(
                            insert_query,
                            {
                                "product_id": product_id,
                                "order_id": order_id,
                                "count": qty,
                                "price": price,
                                "total_sum": qty * price,
                                "bonus_payment": float(p.get("bonus_payment", 0)),
                                "bonus_grant": float(p.get("bonus_grant", 0)),
                            },
                        )

                        inserted += 1

                c.commit()

                log.info(f"Inserted: {inserted}")

    load_product_sales_to_dds()


dds_fct_product_sales_dag = dds_fct_product_sales()

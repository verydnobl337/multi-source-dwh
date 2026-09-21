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
    "dds_dm_orders_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)
def dds_dm_orders():

    @task()
    def load_orders_to_dds():
        log = logging.getLogger(__name__)

        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

        # Получаем заказы из STG в порядке их обновления.
        select_query = """
            SELECT
                id,
                object_value
            FROM stg.ordersystem_orders
            WHERE object_value IS NOT NULL
            ORDER BY update_ts
        """

        # Upsert заказов в DDS: существующие записи обновляются,
        # новые записи добавляются.
        insert_query = """
            INSERT INTO dds.dm_orders(
                order_key,
                order_status,
                restaurant_id,
                timestamp_id,
                user_id
            )
            VALUES(
                %(order_key)s,
                %(order_status)s,
                %(restaurant_id)s,
                %(timestamp_id)s,
                %(user_id)s
            )
            ON CONFLICT (order_key) DO UPDATE SET
                order_status = EXCLUDED.order_status,
                restaurant_id = EXCLUDED.restaurant_id,
                timestamp_id = EXCLUDED.timestamp_id,
                user_id = EXCLUDED.user_id
        """

        # Поиск restaurant_id по оригинальному id ресторана
        # Берем актуальную версию (active_to = '2099-12-31')
        get_restaurant_id_query = """
            SELECT id FROM dds.dm_restaurants 
            WHERE restaurant_id = %(restaurant_origin_id)s
            AND active_to = '2099-12-31'
            LIMIT 1
        """

        # Поиск user_id по оригинальному id пользователя
        get_user_id_query = """
            SELECT id FROM dds.dm_users 
            WHERE user_id = %(user_origin_id)s
            LIMIT 1
        """

        # Поиск timestamp_id по времени заказа
        get_timestamp_id_query = """
            SELECT id FROM dds.dm_timestamps 
            WHERE ts = %(order_date)s
            LIMIT 1
        """

        # Одна транзакция используется для обработки текущего набора данных.
        with dwh_pg_connect.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(select_query)
                records = cur.fetchall()

                log.info(f"Found {len(records)} orders in STG")

                inserted_count = 0
                updated_count = 0
                skipped_count = 0

                for record in records:
                    try:
                        order_json = str2json(record[1])

                        # Извлекаем order_key и order_status
                        order_key = order_json.get("_id", "")
                        if isinstance(order_key, dict):
                            order_key = order_key.get("$oid", "")
                        else:
                            order_key = str(order_key)

                        order_status = order_json.get("final_status", "")

                        if not order_key:
                            log.warning(f"No order key in record {record[0]}")
                            skipped_count += 1
                            continue

                        # Извлекаем restaurant_id
                        restaurant_origin_id = order_json.get("restaurant", {}).get(
                            "id", ""
                        )
                        if not restaurant_origin_id:
                            log.warning(f"No restaurant in order {order_key}")
                            skipped_count += 1
                            continue

                        # Извлекаем user_id
                        user_origin_id = order_json.get("user", {}).get("id", "")
                        if not user_origin_id:
                            # Пробуем другой путь к пользователю
                            user_origin_id = order_json.get("user_id", "")

                        if not user_origin_id:
                            log.warning(f"No user in order {order_key}")
                            skipped_count += 1
                            continue

                        # Извлекаем дату заказа
                        order_date_str = order_json.get("date", "")
                        if not order_date_str:
                            log.warning(f"No date in order {order_key}")
                            skipped_count += 1
                            continue

                        order_date = datetime.strptime(
                            order_date_str, "%Y-%m-%d %H:%M:%S"
                        )

                        # Получаем restaurant_id из dm_restaurants
                        cur.execute(
                            get_restaurant_id_query,
                            {"restaurant_origin_id": restaurant_origin_id},
                        )
                        restaurant_row = cur.fetchone()

                        if not restaurant_row:
                            log.warning(
                                f"Restaurant {restaurant_origin_id} not found in dm_restaurants"
                            )
                            skipped_count += 1
                            continue
                        restaurant_id = restaurant_row[0]

                        # Получаем user_id из dm_users
                        cur.execute(
                            get_user_id_query, {"user_origin_id": user_origin_id}
                        )
                        user_row = cur.fetchone()

                        if not user_row:
                            log.warning(f"User {user_origin_id} not found in dm_users")
                            skipped_count += 1
                            continue
                        user_id = user_row[0]

                        # Получаем timestamp_id из dm_timestamps
                        cur.execute(get_timestamp_id_query, {"order_date": order_date})
                        timestamp_row = cur.fetchone()

                        if not timestamp_row:
                            log.warning(
                                f"Timestamp {order_date} not found in dm_timestamps"
                            )
                            skipped_count += 1
                            continue
                        timestamp_id = timestamp_row[0]

                        # Вставляем или обновляем заказ
                        cur.execute(
                            insert_query,
                            {
                                "order_key": order_key,
                                "order_status": order_status,
                                "restaurant_id": restaurant_id,
                                "timestamp_id": timestamp_id,
                                "user_id": user_id,
                            },
                        )

                        if cur.rowcount > 0:
                            if cur.rowcount == 1:
                                inserted_count += 1
                                if inserted_count <= 10:
                                    log.info(
                                        f"✅ Inserted order: {order_key} (status: {order_status})"
                                    )
                            else:
                                updated_count += 1

                    except Exception as e:
                        log.error(f"Error processing order {record[0]}: {e}")
                        skipped_count += 1
                        continue

                conn.commit()
                log.info(f"📊 FINAL SUMMARY:")
                log.info(f"   - Inserted orders: {inserted_count}")
                log.info(f"   - Updated orders: {updated_count}")
                log.info(f"   - Skipped: {skipped_count}")
                log.info(f"   - Total processed: {len(records)}")

                # Проверяем итоговое количество
                cur.execute("SELECT COUNT(*) FROM dds.dm_orders")
                dds_count = cur.fetchone()[0]
                log.info(f"📊 Total orders in DDS: {dds_count}")

    load_task = load_orders_to_dds()


dds_dm_orders_dag = dds_dm_orders()

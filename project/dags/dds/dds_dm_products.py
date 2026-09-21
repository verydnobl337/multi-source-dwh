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
    "dds_dm_products_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)
def dds_dm_products():

    @task()
    def load_products_to_dds():
        log = logging.getLogger(__name__)

        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

        # Запрос для получения заказов с продуктами
        select_query = """
            SELECT
                id,
                object_value
            FROM stg.ordersystem_orders
            WHERE object_value IS NOT NULL
            ORDER BY update_ts
        """
        
        # Вставка продукта
        insert_query = """
            INSERT INTO dds.dm_products(
                product_id,
                product_name,
                product_price,
                active_from,
                active_to,
                restaurant_id
            )
            VALUES(
                %(product_id)s,
                %(product_name)s,
                %(product_price)s,
                %(active_from)s,
                %(active_to)s,
                %(restaurant_id)s
            )
            ON CONFLICT (product_id, restaurant_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                product_price = EXCLUDED.product_price,
                active_from = EXCLUDED.active_from,
                active_to = EXCLUDED.active_to
        """
        
        # Получение restaurant_id из dm_restaurants по restaurant_origin_id
        get_restaurant_id_query = """
            SELECT id FROM dds.dm_restaurants 
            WHERE restaurant_id = %(restaurant_origin_id)s
            LIMIT 1
        """

        # Обрабатываем заказы и извлекаем из них уникальные продукты.
        with dwh_pg_connect.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(select_query)
                records = cur.fetchall()

                log.info(f"Found {len(records)} orders in STG")

                inserted_count = 0
                updated_count = 0
                skipped_count = 0
                products_set = set()  # Для отслеживания уникальных продуктов

                for record in records:
                    try:
                        order_json = str2json(record[1])
                        
                        # Извлекаем restaurant_id из заказа
                        restaurant_origin_id = order_json.get("restaurant", {}).get("id", "")
                        if not restaurant_origin_id:
                            log.warning(f"No restaurant in order {record[0]}")
                            skipped_count += 1
                            continue
                        
                        # Получаем числовой restaurant_id из dm_restaurants
                        cur.execute(get_restaurant_id_query, {
                            "restaurant_origin_id": restaurant_origin_id
                        })
                        restaurant_db_id = cur.fetchone()
                        
                        if not restaurant_db_id:
                            log.warning(f"Restaurant {restaurant_origin_id} not found in dm_restaurants")
                            skipped_count += 1
                            continue
                        
                        restaurant_id = restaurant_db_id[0]
                        
                        # Извлекаем продукты из заказа.
                        # Структура: order_items или items
                        order_items = order_json.get("order_items", [])
                        if not order_items:
                            order_items = order_json.get("items", [])
                        
                        # Берем update_ts из заказа
                        active_from_str = order_json.get("date", "")
                        if active_from_str:
                            active_from = datetime.strptime(active_from_str, "%Y-%m-%d %H:%M:%S")
                        else:
                            active_from = datetime.now()
                        
                        active_to = datetime(2099, 12, 31, 0, 0, 0)
                        
                        # Обрабатываем каждый продукт в заказе
                        for item in order_items:
                            product_id = item.get("id", "")
                            product_name = item.get("name", "")
                            product_price = item.get("price", 0)
                            
                            if not product_id:
                                continue
                            
                            # Создаем уникальный ключ для отслеживания
                            product_key = f"{product_id}_{restaurant_origin_id}"
                            
                            if product_key in products_set:
                                continue
                            
                            products_set.add(product_key)
                            
                            # Вставляем или обновляем продукт
                            cur.execute(
                                insert_query,
                                {
                                    "product_id": product_id,
                                    "product_name": product_name,
                                    "product_price": product_price,
                                    "active_from": active_from,
                                    "active_to": active_to,
                                    "restaurant_id": restaurant_id,
                                },
                            )
                            
                            if cur.rowcount > 0:
                                # Проверяем, была ли это вставка или обновление
                                if cur.rowcount == 1:
                                    inserted_count += 1
                                    if inserted_count <= 10:
                                        log.info(f"✅ Inserted product: {product_name} (id: {product_id})")
                                else:
                                    updated_count += 1
                        
                    except Exception as e:
                        log.error(f"Error processing order {record[0]}: {e}")
                        skipped_count += 1
                        continue

                conn.commit()
                log.info(f"📊 FINAL SUMMARY:")
                log.info(f"   - Inserted products: {inserted_count}")
                log.info(f"   - Updated products: {updated_count}")
                log.info(f"   - Skipped: {skipped_count}")
                log.info(f"   - Total unique products: {len(products_set)}")
                
                # Проверяем итоговое количество
                cur.execute("SELECT COUNT(*) FROM dds.dm_products")
                dds_count = cur.fetchone()[0]
                log.info(f"📊 Total products in DDS: {dds_count}")

    load_task = load_products_to_dds()

dds_dm_products_dag = dds_dm_products()
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from lib import ConnectionBuilder
from lib.dict_util import str2json
import logging
import os

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    "dds_dm_restaurants_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)
def dds_dm_restaurants():

    @task()
    def load_restaurants_to_dds():
        log = logging.getLogger(__name__)

        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

        select_query = """
            SELECT
                id,
                object_id,
                object_value,
                update_ts
            FROM stg.ordersystem_restaurants
            WHERE object_value IS NOT NULL
            ORDER BY update_ts
        """
        
        insert_query = """
            INSERT INTO dds.dm_restaurants(
                restaurant_id,
                restaurant_name,
                active_from,
                active_to
            )
            VALUES(
                %(restaurant_id)s,
                %(restaurant_name)s,
                %(active_from)s,
                %(active_to)s
            )
            ON CONFLICT (restaurant_id) DO NOTHING
        """

        with dwh_pg_connect.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(select_query)
                records = cur.fetchall()
                
                log.info(f"Found {len(records)} records in STG")

                inserted_count = 0
                
                for record in records:
                    try:
                        restaurant_json = str2json(record[2])
                        
                        restaurant_id_field = restaurant_json.get("_id", {})
                        if isinstance(restaurant_id_field, dict):
                            restaurant_id = restaurant_id_field.get("$oid", "")
                        else:
                            restaurant_id = str(restaurant_id_field)

                        restaurant_name = restaurant_json.get("name", "")
                        
                        active_from = record[3]

                        active_to = datetime(2099, 12, 31, 0, 0, 0)
                        log.info(f"Processing restaurant: {restaurant_name}, ID: {restaurant_id}, active_from: {active_from}")

                        cur.execute(
                            insert_query,
                            {
                                "restaurant_id": restaurant_id,
                                "restaurant_name": restaurant_name,
                                "active_from": active_from,
                                "active_to": active_to
                            },
                        )
                    except Exception as e:
                        log.error(f"Error processing record {record[0]}: {e}")
                        log.error(f"Problem JSON: {record[2][:200]}...")
                        continue

                conn.commit()
                log.info(f"Summary: Inserted {inserted_count}, Total {len(records)}")

    load_task = load_restaurants_to_dds()


dds_dm_restaurants_dag = dds_dm_restaurants()
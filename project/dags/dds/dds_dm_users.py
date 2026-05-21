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
    "dds_dm_users_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)
def dds_dm_users():

    @task()
    def load_users_to_dds():
        log = logging.getLogger(__name__)

        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        
        select_query = """
            SELECT
                id,
                object_id,
                object_value,
                update_ts
            FROM stg.ordersystem_users
            WHERE object_value IS NOT NULL
            ORDER BY update_ts
        """
        
        insert_query = """
            INSERT INTO dds.dm_users(
                user_id,
                user_name,
                user_login
            )
            VALUES(
                %(user_id)s,
                %(user_name)s,
                %(user_login)s
            )
            ON CONFLICT (user_id) DO UPDATE SET
                user_name = EXCLUDED.user_name,
                user_login = EXCLUDED.user_login
        """

        with dwh_pg_connect.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(select_query)
                records = cur.fetchall()
                
                log.info(f"Found {len(records)} records in STG")

                inserted_count = 0
                updated_count = 0
                
                for record in records:
                    try:
                        user_json = str2json(record[2])
                        
                        user_id_field = user_json.get("_id", {})
                        if isinstance(user_id_field, dict):
                            user_id = user_id_field.get("$oid", "")
                        else:
                            user_id = str(user_id_field)

                        user_name = user_json.get("name", "")
                        user_login = user_json.get("login", "")
                        
                        # Пропускаем записи без login (это не пользователи)
                        if not user_login:
                            continue
                        
                        log.info(f"Processing user: {user_login}, ID: {user_id}")

                        cur.execute(
                            insert_query,
                            {
                                "user_id": user_id,
                                "user_name": user_name,
                                "user_login": user_login,
                            },
                        )
                        
                        if cur.rowcount > 0:
                            if cur.rowcount == 1:
                                inserted_count += 1
                                log.info(f"Inserted user: {user_login}")
                            else:
                                updated_count += 1
                                log.info(f"Updated user: {user_login}")

                    except Exception as e:
                        log.error(f"Error processing record {record[0]}: {e}")
                        log.error(f"Problem JSON: {record[2][:200]}...")
                        continue

                conn.commit()
                log.info(f"Summary: Inserted {inserted_count}, Updated {updated_count}, Total {len(records)}")

    load_task = load_users_to_dds()


dds_dm_users_dag = dds_dm_users()

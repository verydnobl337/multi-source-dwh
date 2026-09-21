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
    "dds_dm_timestamps_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)
def dds_dm_timestamps():

    @task()
    def add_unique_constraint():
        """Добавляет уникальное ограничение, если его нет"""
        log = logging.getLogger(__name__)
        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        
        # Проверяем существование ограничения
        check_constraint = """
            SELECT 1 FROM pg_constraint 
            WHERE conname = 'dm_timestamps_ts_unique'
            AND conrelid = 'dds.dm_timestamps'::regclass
        """
        
        add_constraint = """
            ALTER TABLE dds.dm_timestamps 
            ADD CONSTRAINT dm_timestamps_ts_unique UNIQUE (ts)
        """
        
        with dwh_pg_connect.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(check_constraint)
                exists = cur.fetchone()
                
                if not exists:
                    log.info("Adding unique constraint on ts column...")
                    try:
                        cur.execute(add_constraint)
                        conn.commit()
                        log.info("✅ Unique constraint added successfully")
                    except Exception as e:
                        log.error(f"Failed to add constraint: {e}")
                        raise
                else:
                    log.info("Unique constraint already exists")

    @task()
    def load_timestamps_to_dds():
        # Формируем календарные значения из дат заказов.
        # Они используются DDS и CDM для агрегации по периодам.
        log = logging.getLogger(__name__)

        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

        select_query = """
            SELECT
                id,
                object_value
            FROM stg.ordersystem_orders
            WHERE object_value IS NOT NULL
            ORDER BY update_ts
        """

        insert_query = """
            INSERT INTO dds.dm_timestamps(
                ts,
                year,
                month,
                day,
                time,
                date
            )
            VALUES(
                %(ts)s,
                %(year)s,
                %(month)s,
                %(day)s,
                %(time)s,
                %(date)s
            )
            ON CONFLICT (ts) DO NOTHING
        """

        with dwh_pg_connect.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(select_query)
                records = cur.fetchall()

                log.info(f"Found {len(records)} records in STG")

                inserted_count = 0
                skipped_status_count = 0
                skipped_no_date_count = 0
                error_count = 0

                for record in records:
                    try:
                        order_json = str2json(record[1])

                        final_status = order_json.get("final_status", "")
                        if final_status not in ["CLOSED", "CANCELLED"]:
                            skipped_status_count += 1
                            continue

                        date_str = order_json.get("date", "")
                        if not date_str:
                            skipped_no_date_count += 1
                            continue

                        # Парсим дату
                        ts = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

                        year = ts.year
                        month = ts.month
                        day = ts.day
                        date_only = ts.date()
                        time_only = ts.time()

                        # Выполняем вставку
                        cur.execute(
                            insert_query,
                            {
                                "ts": ts,
                                "year": year,
                                "month": month,
                                "day": day,
                                "date": date_only,
                                "time": time_only,
                            },
                        )
                        
                        if cur.rowcount > 0:
                            inserted_count += 1
                            if inserted_count <= 10:
                                log.info(f"✅ Inserted: {ts} (status: {final_status})")

                    except ValueError as e:
                        log.error(f"ValueError for record {record[0]}: {e}")
                        error_count += 1
                        continue
                    except Exception as e:
                        log.error(f"Error processing record {record[0]}: {e}")
                        error_count += 1
                        continue

                conn.commit()
                log.info(f"📊 FINAL SUMMARY:")
                log.info(f"   - Inserted: {inserted_count}")
                log.info(f"   - Skipped (wrong status): {skipped_status_count}")
                log.info(f"   - Skipped (no date): {skipped_no_date_count}")
                log.info(f"   - Errors: {error_count}")
                log.info(f"   - Total processed: {len(records)}")
                
                # Проверяем итоговое количество
                cur.execute("SELECT COUNT(*) FROM dds.dm_timestamps")
                dds_count = cur.fetchone()[0]
                log.info(f"📊 Total timestamps in DDS: {dds_count}")

    # Запускаем задачи последовательно
    add_constraint_task = add_unique_constraint()
    load_task = load_timestamps_to_dds()
    
    add_constraint_task >> load_task


dds_dm_timestamps_dag = dds_dm_timestamps()
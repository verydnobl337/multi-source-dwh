from airflow.decorators import dag, task
from airflow.models.variable import Variable
from datetime import datetime, timedelta
from examples.stg.order_system_order_dag.pg_saver import OrdersPgSaver
from examples.stg.order_system_order_dag.orders_reader import OrdersReader
from examples.stg.order_system_order_dag.orders_loader import OrdersLoader
from lib import ConnectionBuilder, MongoConnect
import logging

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    "load_orders_from_mongodb",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)
def stg_order_system_orders():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_orders():
        log = logging.getLogger(__name__)

        pg_saver = OrdersPgSaver()

        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        collection_reader = OrdersReader(mongo_connect)

        loader = OrdersLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        loader.run_copy()

    orders_loader = load_orders()

    orders_loader


order_stg_dag = stg_order_system_orders()

from airflow.decorators import dag, task
from datetime import datetime
from globals import get_postgres_client


@dag(default_args={'owner': 'airflow', 'start_date': datetime.today()},
     schedule_interval=None,
     catchup=False)
def init_db():
    @task()
    def create_pg_tables():
        pg = get_postgres_client()
        pg.run(open("/opt/airflow/sql/pg/create_tables.sql").read())
        pg.run(open("/opt/airflow/sql/dds/create_tables.sql").read())
    create_pg_tables()
init_db()

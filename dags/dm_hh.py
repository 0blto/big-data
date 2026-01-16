from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.sensors.external_task import ExternalTaskSensor
from globals import get_postgres_client

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id="dm_hh_etl",
    default_args=DEFAULT_ARGS,
    description="ETL for HH data marts (DM layer)",
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['dm', 'hh']
)
def dm_hh_etl():

    # Ждём, пока полностью отработает DDS
    wait_for_test = ExternalTaskSensor(
        task_id="wait_for_test",
        external_dag_id="dds_data_quality",
        external_task_id="compare_row_counts",
        mode="reschedule",
        poke_interval=10,
        timeout=60 * 60 * 24
    )

    @task
    def build_data_marts():
        pg = get_postgres_client()
        sql = open("/opt/airflow/sql/dm/create_tables.sql").read()
        pg.run(sql)

    wait_for_test >> build_data_marts()


dag_instance = dm_hh_etl()
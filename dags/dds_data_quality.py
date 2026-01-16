from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
from globals import get_postgres_client
import logging
from airflow.sensors.external_task import ExternalTaskSensor

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="dds_data_quality",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=['dds', 'data_quality']
)
def dds_data_quality():
    wait_for_dds = ExternalTaskSensor(
        task_id="wait_for_dds",
        external_dag_id="dds_hh_etl",
        external_task_id="transform_and_load",
        mode="reschedule",
        poke_interval=5,
        timeout=60 * 60 * 24
    )

    @task
    def check_vacancy_current_unique():
        client = get_postgres_client()
        result = client.run(
            """
            SELECT vacancy_bk, COUNT(*) FROM dim_vacancy
            WHERE is_current = true
            GROUP BY vacancy_bk
            HAVING COUNT(*) > 1
            """
        )
        if result:
            raise ValueError(f"Найдены дубликаты текущих вакансий: {result}")
        logging.info("Проверка dim_vacancy passed")

    @task
    def check_fact_references():
        client = get_postgres_client()
        errors = []
        vac_missing = client.run(
            """
            SELECT f.fact_sk FROM fact_salary f
            LEFT JOIN dim_vacancy v ON f.vacancy_sk = v.vacancy_sk
            WHERE v.vacancy_sk IS NULL
            """
        )
        if vac_missing:
            errors.append(f"Найдены факты без вакансии: {vac_missing}")

        loc_missing = client.run(
            """
            SELECT f.fact_sk FROM fact_salary f
            LEFT JOIN dim_location l ON f.location_sk = l.location_sk
            WHERE f.location_sk IS NOT NULL AND l.location_sk IS NULL
            """
        )
        if loc_missing:
            errors.append(f"Найдены факты с несуществующими локациями: {loc_missing}")

        date_missing = client.run(
            """
            SELECT f.fact_sk FROM fact_salary f
            LEFT JOIN dim_date d ON f.date_sk = d.date_sk
            WHERE f.date_sk IS NOT NULL AND d.date_sk IS NULL
            """
        )
        if date_missing:
            errors.append(f"Найдены факты с несуществующими датами: {date_missing}")

        if errors:
            raise ValueError("Ошибки ссылочной целостности: " + " | ".join(errors))
        logging.info("Проверка fact_salary passed")

    @task
    def check_salary_ranges():
        client = get_postgres_client()
        invalid_salaries = client.run(
            """
            SELECT fact_sk, salary_from, salary_to FROM fact_salary
            WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL AND salary_from > salary_to
            """
        )
        if invalid_salaries:
            raise ValueError(f"Неверные диапазоны зарплат: {invalid_salaries}")
        logging.info("Проверка salary ranges passed")

    @task
    def check_anomalies():
        client = get_postgres_client()

        invalid_salaries = client.run(
            """
            SELECT fact_sk, salary_from, salary_to
            FROM fact_salary
            WHERE salary_from < 0
               OR salary_to < 0
               OR salary_from > 1000000
               OR salary_to > 1000000
            """
        )
        if invalid_salaries:
            raise ValueError(f"Обнаружены аномальные значения зарплат: {invalid_salaries}")

        missing_fields = client.run(
            """
            SELECT vacancy_sk
            FROM dim_vacancy
            WHERE vacancy_name_sk IS NULL
               OR company_sk IS NULL
               OR vacancy_url IS NULL
            """
        )
        if missing_fields:
            raise ValueError(f"Обнаружены пропущенные ключевые поля: {missing_fields}")

        logging.info("Проверка anomaly check passed")

    @task
    def compare_row_counts():
        client = get_postgres_client()
        ods_count = client.get_first("SELECT COUNT(*) as cnt FROM ods_hh_vacancies")['cnt']
        dds_count = client.get_first("SELECT COUNT(*) as cnt FROM fact_salary")['cnt']

        logging.info(f"ODS rows: {ods_count}, DDS rows: {dds_count}")

        if dds_count / ods_count < 0.8:
            raise ValueError(f"Несоответствие количества строк: ODS={ods_count}, DDS={dds_count}")
        logging.info("ODS rows passed")

    wait_for_dds >> check_vacancy_current_unique() >> check_fact_references() >> check_salary_ranges() >> check_anomalies() >> compare_row_counts()

dds_data_quality_dag = dds_data_quality()

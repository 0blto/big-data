import re

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from typing import Dict, Any, List
from globals import get_postgres_client
import hashlib
from airflow.sensors.external_task import ExternalTaskSensor
from globals.utils import parse_datetime

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

general_patterns = {
    "developer / engineer": [
        r"engineer", r"developer", r"разработчик", r"инженер", r"programmer",
        r"python", r"java", r"golang", r"ml", r"data"
    ],
    "manager / lead": [
        r"manager", r"lead", r"руководитель", r"директор", r"coo", r"cto", r"cio",
        r"head", r"chief officer", r"pm", r"product owner", r"product manager"
    ],
    "hr / recruiter": [r"hr", r"recruiter", r"people & culture", r"персонал"],
    "qa / tester": [r"qa", r"тестировщик", r"testing"],
    "support / ops": [r"support", r"technical support", r"саппорт", r"оператор", r"helpdesk"],
    "legal / finance": [r"legal", r"юрист", r"бухгалтер", r"finance", r"финансовый"],
    "driver / logistics": [r"driver", r"водитель", r"экспедитор"],
    "designer / content": [r"designer", r"дизайнер", r"копирайтер", r"технический писатель", r"content"],
}

def normalize_title(title: str) -> str:
    title_lower = title.lower()
    for category, patterns in general_patterns.items():
        for pattern in patterns:
            if re.search(pattern, title_lower):
                return category
    return "other / miscellaneous"

def generate_bk(*args) -> int:
    hash_input = "|".join([str(a) for a in args])
    return int(hashlib.md5(hash_input.encode()).hexdigest(), 16) % (10 ** 10)


@dag(
    dag_id="dds_hh_etl",
    default_args=DEFAULT_ARGS,
    description="ETL from ODS HH vacancies to DDS",
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['dds', 'hh']
)
def dds_hh_etl():
    wait_for_ods = ExternalTaskSensor(
        task_id="wait_for_ods",
        external_dag_id="ods_hh_etl",
        external_task_id="load",
        mode="reschedule",
        poke_interval=5,
        timeout=60 * 60 * 24
    )

    @task
    def extract_vacancies() -> List[Dict[str, Any]]:
        client = get_postgres_client()
        return client.select("ods_hh_vacancies", where="archived = false")

    @task
    def transform_and_load(vacancies: List[Dict[str, Any]]):
        client = get_postgres_client()

        for vac in vacancies:

            city_bk = generate_bk(vac.get("city"))
            client.insert_ignore(
                table="dim_city",
                data={
                    "city_bk": city_bk,
                    "city_name": vac.get("city")
                }
            )
            city_sk = client.get_first(
                "SELECT city_sk FROM dim_city WHERE city_bk = %s",
                (city_bk,)
            )["city_sk"]

            location_bk = generate_bk(vac.get("location"), vac.get("city"))
            client.insert_ignore(
                table="dim_location",
                data={
                    "location_bk": location_bk,
                    "city_sk": city_sk,
                    "address": vac.get("location")
                }
            )
            location_sk = client.get_first(
                "SELECT location_sk FROM dim_location WHERE location_bk = %s",
                (location_bk,)
            )["location_sk"]

            company_bk = generate_bk(vac.get("employer"))
            client.run(
                """
                INSERT INTO dim_company (company_bk, company_name)
                VALUES (%s, %s) ON CONFLICT (company_bk) DO NOTHING
                """,
                (company_bk, vac.get("employer"))
            )
            company_sk = client.get_first(
                "SELECT company_sk FROM dim_company WHERE company_bk = %s",
                (company_bk,)
            )["company_sk"]

            title = vac.get("title")
            normalized_title = normalize_title(title)

            title_bk = generate_bk(normalized_title)
            client.insert_versioned(
                table="dim_vacancy_name",
                key_column="vacancy_name_sk",
                key_value=title_bk,
                data={"title": normalized_title}
            )
            vacancy_name_sk = client.get_first(
                "SELECT vacancy_name_sk FROM dim_vacancy_name WHERE vacancy_name_sk = %s AND is_current = true",
                (title_bk,)
            )["vacancy_name_sk"]

            vacancy_bk = generate_bk(vac.get("vacancy_id"))
            client.insert_versioned(
                table="dim_vacancy",
                key_column="vacancy_bk",
                key_value=vacancy_bk,
                data={
                    "vacancy_name_sk": vacancy_name_sk,
                    "company_sk": company_sk,
                    "vacancy_url": vac.get("url")
                }
            )
            vacancy_sk = client.get_first(
                "SELECT vacancy_sk FROM dim_vacancy WHERE vacancy_bk = %s AND is_current = true",
                (vacancy_bk,)
            )["vacancy_sk"]

            published_at = parse_datetime(vac.get("published_at"))
            if published_at:
                date_bk = published_at.date()
                client.insert_ignore(
                    table="dim_date",
                    data={
                        "date_bk": date_bk,
                        "day": date_bk.day,
                        "month": date_bk.month,
                        "quarter": (date_bk.month - 1) // 3 + 1,
                        "year": date_bk.year,
                        "day_of_week": date_bk.isoweekday()
                    }
                )
                date_sk = client.get_first(
                    "SELECT date_sk FROM dim_date WHERE date_bk = %s",
                    (date_bk,)
                )["date_sk"]
            else:
                date_sk = None

            client.insert_ignore(
                table="fact_salary",
                data={
                    "vacancy_sk": vacancy_sk,
                    "location_sk": location_sk,
                    "date_sk": date_sk,
                    "salary_from": vac.get("salary_from"),
                    "salary_to": vac.get("salary_to"),
                    "salary_currency": vac.get("salary_currency"),
                    "archived": vac.get("archived", False),
                    "created_at": datetime.now()
                }
            )
    vacancies = extract_vacancies()
    wait_for_ods >> vacancies
    transform_and_load(vacancies)



dag_instance = dds_hh_etl()

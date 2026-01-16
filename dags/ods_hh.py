import json

from airflow.decorators import dag, task
from datetime import timedelta
import requests, logging, random
from globals import get_postgres_client
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago


HH_URL = "https://api.hh.ru/vacancies"
SEARCH_QUERY = "it"
AREAS = {
    "Москва": "1",
    "Санкт-Петербург": "2",
    "Новосибирск": "4",
    "Екатеринбург": "66",
    "Казань": "88",
    "Самара": "75",
    "Ростов-на-Дону": "61"
}

@dag(
    dag_id="ods_hh_etl",
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "start_date": days_ago(1)
    },
    schedule_interval="@daily",
    catchup=False,
    tags=["hh", "vacancies", "etl"],
    max_active_runs=1
)
def ods_hh_etl():
    @task(task_id="extract")
    def extract():
        all_vacancies = []

        for city, area_id in AREAS.items():
            params = {"text": SEARCH_QUERY, "area": area_id, "per_page": 100}
            r = requests.get(HH_URL, params=params)
            data = r.json().get("items", [])
            logging.info(f"{city}: Получено {len(data)} вакансий")

            for v in data:
                add = v.get("address")
                metro = add.get("metro") if add else None
                station = metro.get("station_name") if metro else None

                all_vacancies.append({
                    "vacancy_id": v["id"],
                    "title": v.get("name"),
                    "city": city,
                    "location": station,
                    "salary": v.get("salary"),
                    "employer": v.get("employer", {}).get("name"),
                    "url": v.get("alternate_url"),
                    "archived": v.get("archived", False),
                    "published_at": v.get("published_at"),
                    "raw": v
                })

        return all_vacancies

    @task(task_id="load")
    def load(vacancies):
        pg = get_postgres_client()

        for v in vacancies:
            sal = v.get("salary") or {}
            salary_from = sal.get("from")
            salary_to = sal.get("to")

            if salary_from: salary_from += random.randint(1000, 10000) if random.random() < 0.2 else 0
            if salary_to: salary_to += random.randint(10000, 50000) if random.random() < 0.2 else 0

            pg.insert(
                table="ods_hh_vacancies",
                data={
                    "vacancy_id": v["vacancy_id"],
                    "title": v["title"],
                    "city": v["city"],
                    "location": v["location"],
                    "salary_from": salary_from,
                    "salary_to": salary_to,
                    "salary_currency": sal.get("currency"),
                    "employer": v["employer"],
                    "url": v["url"],
                    "archived": v["archived"],
                    "published_at": v["published_at"],
                    "raw": json.dumps(v["raw"], ensure_ascii=False)
                }
            )

        logging.info(f"Добавлено/обновлено вакансий: {len(vacancies)}")

    vacancies = extract()
    load(vacancies)

ods_hh_etl()

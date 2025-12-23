from airflow.decorators import dag, task
from datetime import datetime
import requests
from globals import get_mongo_client

API_URL = "http://opendata.trudvsem.ru/api/v1/vacancies"

@dag(
    default_args={'owner': 'airflow', 'start_date': datetime(2024, 1, 1)},
    schedule_interval='@daily',
    catchup=False,
    tags=['trudvsem']
)
def simple_trudvsem_etl():
    @task()
    def extract_and_load():
        response = requests.get(API_URL, params={"text": "Data Scientist", "limit": 100})
        data = response.json()
        vacancies = data.get("results", {}).get("vacancies", [])
        mongo = get_mongo_client()
        for item in vacancies:
            if 'vacancy' in item:
                vacancy = item['vacancy']
                mongo.upsert_versioned(
                    "ods_mongo_db",
                    "vacancies_ds",
                    {"id": vacancy.get("id")},
                    vacancy
                )
        print(f"Сохранили {len(vacancies)} вакансий")
    extract_and_load()
simple_trudvsem_etl()

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests, json, logging
from globals import get_postgres_client, get_mongo_client, get_minio_client

HH_URL = "https://api.hh.ru/vacancies"
SEARCH_QUERY = "data engineer"
AREAS = {"Москва": "1", "Санкт-Петербург": "2"}
BUCKET = "hh-vacancies"

MONGO_DB = "ods_mongo_db"
MONGO_RAW_COLL = "hh_vacancies"


@dag(
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "start_date": datetime(2025, 1, 1)
    },
    schedule_interval="@daily",
    catchup=False,
    tags=["hh", "vacancies", "etl"]
)
def hh_vacancies_etl():
    @task()
    def extract():
        all_vacancies = []
        for city, area_id in AREAS.items():
            params = {"text": SEARCH_QUERY, "area": area_id, "per_page": 50}
            r = requests.get(HH_URL, params=params)
            data = r.json().get("items", [])
            logging.info(data[0:2])

            for v in data:
                all_vacancies.append({
                    "vacancy_id": v["id"],
                    "title": v.get("name"),
                    "city": city,
                    "salary": v.get("salary"),
                    "employer": v.get("employer", {}).get("name"),
                    "published_at": v.get("published_at"),
                    "url": v.get("alternate_url"),
                    "raw": v
                })

        return all_vacancies

    @task()
    def load(vacancies):
        pg = get_postgres_client()
        mongo = get_mongo_client()
        s3 = get_minio_client()

        for v in vacancies:
            sal = v.get("salary") or {}
            mongo.upsert_versioned(MONGO_DB, MONGO_RAW_COLL, {"vacancy_id": v["vacancy_id"]}, v)
            s3.upsert_object(BUCKET, f"{v['vacancy_id']}.json", json.dumps(v["raw"]).encode())
            pg.upsert("hh_vacancies", {
                "vacancy_id": v["vacancy_id"],
                "title": v["title"],
                "city": v["city"],
                "salary_from": sal.get("from"),
                "salary_to": sal.get("to"),
                "employer": v["employer"],
                "published_at": v["published_at"],
                "url": v["url"]
            }, conflict_column="vacancy_id")

        logging.info(f"Loaded {len(vacancies)} vacancies")

    load(extract())


hh_vacancies_etl()


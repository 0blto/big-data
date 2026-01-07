import hashlib

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging, time, json, re
from globals import get_postgres_client, get_mongo_client, get_minio_client
from globals.selenium_utils import get_selenium_driver, safe_get
from selenium.webdriver.common.by import By

CITIES = {"Москва": "moskva", "Санкт-Петербург": "sankt-peterburg"}
BUCKET = "avito-vacancies"
MONGO_DB, MONGO_COLL = "ods_mongo_db", "avito_vacancies"

SEARCH_QUERY = "Data Engineer"

def parse_salary(salary_text: str):
    text = salary_text.replace("\xa0", " ").replace("\u202f", " ").strip()

    match = re.search(r'(\d[\d\s]*)\s*[—-]?\s*(\d[\d\s]*)?\s*(.*)', text)
    if match:
        salary_from = int(match.group(1).replace(" ", "")) if match.group(1) else None
        salary_to = int(match.group(2).replace(" ", "")) if match.group(2) else salary_from
        salary_type = match.group(3).strip() if match.group(3) else None
        return salary_from, salary_to, salary_type
    return None, None, None

@dag(
    default_args={'owner': 'airflow', 'start_date': datetime(2025,1,1), 'retries': 2, 'retry_delay': timedelta(minutes=2)},
    schedule_interval='@daily',
    catchup=False,
    tags=['avito', 'vacancies', 'etl']
)
def avito_vacancies_etl():

    @task()
    def scrape_and_save():
        pg, s3, mongo, drv = get_postgres_client(), get_minio_client(), get_mongo_client(), get_selenium_driver()

        for city, slug in CITIES.items():
            drv.get(f"https://www.avito.ru/{slug}/rabota?q=%22{SEARCH_QUERY}%22")
            time.sleep(3)
            ads = drv.find_elements(By.CSS_SELECTOR, "div.js-catalog-item-enum")
            logging.info(f"{city}: найдено {len(ads)} вакансий")

            for ad in ads[:50]:
                try:
                    url = safe_get(ad, "a[data-marker='item-title']", attr="href")

                    if not url: continue
                    salary = ad.find_element(By.CSS_SELECTOR, "p[data-marker='item-price'] span").text
                    salary_from, salary_to, salary_type = parse_salary(salary)

                    doc = {
                        "url": url,
                        "city": city,
                        "title": safe_get(ad, "a[data-marker='item-title']"),
                        "location": safe_get(ad, "div[data-marker='item-location'] p span:nth-of-type(2)"),
                        "description": safe_get(
                            ad,
                            "div > div > div.iva-item-body-oMJBI > div:nth-child(4) > p"
                        ),
                        "salary": {
                            "from": salary_from,
                            "to": salary_to,
                            "type": salary_type
                        },
                        "raw_html": ad.get_attribute("innerHTML")
                    }

                    mongo.upsert_versioned(MONGO_DB, MONGO_COLL, {"url": doc["url"]}, doc)
                    s3.upsert_object(BUCKET, f"{hashlib.md5(url.encode()).hexdigest()}.json", json.dumps(doc).encode())
                    pg.upsert("avito_vacancies", {
                        "url": doc["url"],
                        "city": doc["city"],
                        "title": doc["title"],
                        "location": doc["location"],
                        "description": doc["description"],
                        "salary_from": doc["salary"]["from"],
                        "salary_to": doc["salary"]["to"],
                        "salary_type": doc["salary"]["type"]
                    }, conflict_column="url")

                except Exception as e:
                    logging.warning(f"Ошибка вакансии: {e}")

        drv.quit()

    scrape_and_save()

avito_vacancies_etl()

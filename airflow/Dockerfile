FROM apache/airflow:2.10.2-python3.12

USER root
RUN apt-get update && apt-get install -y wget unzip
COPY ../requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
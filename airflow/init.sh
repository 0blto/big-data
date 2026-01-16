#!/bin/bash
set -e

echo "Initializing Airflow DB..."
airflow db init

echo "Creating admin user..."
airflow users create \
  --username "${AIRFLOW_USER}" \
  --firstname "${AIRFLOW_FIRSTNAME}" \
  --lastname "${AIRFLOW_LASTNAME}" \
  --role Admin \
  --email "${AIRFLOW_EMAIL}" \
  --password "${AIRFLOW_PASSWORD}" || true

echo "Adding connections..."
airflow connections add ods_postgres --conn-uri "${AIRFLOW_CONN_ODS_POSTGRES}" || true
airflow connections add ods_mongo --conn-uri "${AIRFLOW_CONN_ODS_MONGO}" || true

echo "Initialization complete!"

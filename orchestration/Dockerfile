ARG AIRFLOW_VERSION=2.8.1
FROM apache/airflow:${AIRFLOW_VERSION}

RUN pip install --no-cache-dir \
    "apache-airflow-providers-google" \
    "dbt-core>=1.7.0" \
    "dbt-bigquery>=1.7.0" \
    "google-cloud-bigquery"

# Airflow kullanıcısına geri dönüyoruz
USER airflow
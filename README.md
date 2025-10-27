# Project Overview

This project implements a fully automated ELT (Extract, Load, Transform) data pipeline designed to handle the ingestion of Raw Data files into Google Cloud Storage (GCS) and their subsequent transformation into analytical data models within Google BigQuery using dbt (data build tool). The entire workflow is orchestrated by Apache Airflow. Detailed descriptions of the data models and staging jobs can be found in the separate README files located within their respective folders.


# Architectural Design Decisions Summary

The project’s ELT pipeline is built upon a robust, modern architecture prioritizing scalability and efficiency. Apache Airflow serves as the central orchestrator, managing task dependencies, monitoring, and scheduling, with local deployment handled via Docker Compose. Google BigQuery was selected as the core Data Warehouse for its serverless design and superior analytical query performance. dbt (data build tool) is utilized for all transformation logic, embedding software engineering principles (testing, documentation, dependency management) directly into SQL. For the data loading process, GCS is employed as a cost-effective staging layer where Python scripts handle file optimization (CSV to Parquet). Crucially, security is maintained by mounting the Service Account Key files directly into the Docker containers, establishing a single, secure authentication path for all GCP interactions. Finally, data efficiency within BigQuery is maximized through the use of Time Partitioning and Clustering for large datasets, complemented by an Incremental Merge strategy for dimension tables to minimize reprocessing and reduce operational costs.

## Directory Structure

The project is divided into three main components under the main directory:

```bash
BEIJE_CASE/
├── beije_dbt/          # dbt transformation logic (models, tests, profiles.yml)
├── ingest/             # Data ingestion and preparation code
│   └── staging_jobs/
│       ├── raw/        # Raw CSV files
│       └── etl_stage.py # The GCS upload script (E & L steps)
└── orchestration/      # Airflow environment (Docker, DAGs, Configuration)
    ├── dags/           # Airflow DAG files (beije_dbt_elt_pipeline.py)
    ├── .env
    ├── docker-compose.yml
    └── Dockerfile
```

## 1- Setup and Execution
This project is configured to run locally using Docker and Docker Compose.

### Prerequisites
- Docker and Docker Compose

- Google Cloud Platform (GCP) Project and GCS Bucket

- Python 3.8+ 

## 2-  Service Account (SA) and Authentication
The pipeline requires a Service Account (SA) for authenticating access to GCP resources (GCS and BigQuery).
    1. SA Roles: The Service Account must have the following roles assigned:

        -BigQuery Data Editor

        -BigQuery Job User

        -Storage Object Creator (Crucial for GCS uploads)

- Key File: The SA's JSON key file is used for authentication.
 
- Volume & Env Vars: The SA key file is bound to the containers via two mechanisms:

- Docker Volume: Binds the local file to the container for access by dbt (via profiles.yml):

```bash
- /Users/emresozen/Desktop/beije-476309-4baa1ef43258.json:/opt/airflow/gcp/sa-key.json
```
- Environment Variable: Sets the path for Python/Google SDKs (used by etl_stage.py):
```bash
GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/gcp/sa-key.json
```

## 3-  Starting the Airflow Environment

Navigate to the orchestration folder and set up the environment:
```bash
# 1. UID Setting: Check your .env file and ensure AIRFLOW_UID is set to your local UID (echo $(id -u)).
# AIRFLOW_UID=1000

# 2. Build the Docker images (installs dbt and dependencies)
docker-compose build

# 3. Start all Airflow services (webserver, scheduler, postgres, redis)
docker-compose up -d
```

Airflow UI: http://localhost:8080 (Username/Password: airflow/airflow)

## Pipeline Components

## 1. ETL Ingestion (etl_stage)
This is the first step in the pipeline, executing the Python script ingest/staging_jobs/etl_stage.py.
 - Function: Reads raw CSV files from the local mounted volume (ingest/staging_jobs/raw), converts them to Parquet, and uploads them to the GCS staging bucket (gs://beije-476309-staging-bucket/).

- Command: python /opt/airflow/ingest/staging_jobs/etl_stage.py

- Authentication: Uses the GOOGLE_APPLICATION_CREDENTIALS environment variable to locate the SA key file.

## 2. dbt Transformations
Once data is in GCS, dbt takes over to transform the data in BigQuery.

## 3. dbt Configuration
dbt connectivity is configured via the profiles.yml file located in the project root (beije_dbt/profiles.yml).
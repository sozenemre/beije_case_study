from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_ROOT_PATH = "/usr/local/airflow/dbt/beije_dbt" 

DBT_PROFILE = "beije_dbt_project" 

DBT_COMMAND_PREFIX = f"cd {DBT_PROJECT_ROOT_PATH} && dbt --profile {DBT_PROFILE}"

ETL_STAGE_PATH = "/opt/airflow/ingest/staging_jobs/etl_stage.py"

with DAG(
    dag_id="beije_dbt_elt_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dbt", "elt", "bigquery"],
) as dag:

    etl_stage_task = BashOperator(
        task_id="etl_stage_ingestion",
        # Konteyner içindeki Python 3 ile dosyayı çalıştır
        bash_command=f"python {ETL_STAGE_PATH}",
    )

    dbt_run_ods = BashOperator(
        task_id="dbt_run_ods_layer",
        bash_command=f"{DBT_COMMAND_PREFIX} run --models tag:ods", 
    )

    dbt_run_dim_fact = BashOperator(
        task_id="dbt_run_dim_fact_layer",
        bash_command=f"{DBT_COMMAND_PREFIX} run --models tag:dim tag:fact",
    )
    
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts_layer",
        bash_command=f"{DBT_COMMAND_PREFIX} run --models tag:mart",
    )
    
    (
        etl_stage_task
        >>dbt_run_ods 
        >> dbt_run_dim_fact
        >> dbt_run_marts
    )
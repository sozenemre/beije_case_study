import pandas as pd
import pyarrow
from google.cloud import storage, bigquery
from datetime import datetime
import os
import io
import logging
import glob

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def extract_transform(source_csv_file: str, ingest_date_str: str, csv_read_params: dict = {}) -> io.BytesIO:

    log.info(f"Reading '{source_csv_file}'...")
    try:
        df = pd.read_csv(source_csv_file, **csv_read_params)
    except FileNotFoundError:
        log.error(f"ERROR: Source file not found: '{source_csv_file}'")
        return None
    except Exception as e:
        log.error(f"Error reading CSV ({source_csv_file}): {e}")
        return None

    log.info(f"{len(df)} rows read. Converting to Parquet format...")
    
    df['ingest_date'] = pd.to_datetime(ingest_date_str)
    
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
    parquet_buffer.seek(0)
    
    log.info("Parquet conversion complete.")
    return parquet_buffer

def load_to_gcs(file_buffer: io.BytesIO, gcs_bucket_name: str, gcs_blob_path: str, project_id: str) -> str:
    
    gcs_uri = f"gs://{gcs_bucket_name}/{gcs_blob_path}"
    log.info(f"Load (GCS) step started: Uploading to '{gcs_uri}'...")
    
    try:
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(gcs_bucket_name)
        blob = bucket.blob(gcs_blob_path)
        
        blob.upload_from_file(file_buffer, content_type='application/octet-stream')
        
        log.info(f"File successfully uploaded to GCS: {gcs_uri}")
        return gcs_uri
    except Exception as e:
        log.error(f"GCS upload failed: {e}")
        return None

def load_to_bigquery(gcs_uri: str, project_id: str, bq_dataset_id: str, target_table_id: str) -> bool:
    
    table_ref_str = f"{project_id}.{bq_dataset_id}.{target_table_id}"
    log.info(f"Load (BigQuery) step started: Loading to table '{table_ref_str}'...")
    
    try:
        bq_client = bigquery.Client(project=project_id)

        gcs_prefix = f"gs://beije-476309-staging-bucket/{target_table_id}/"
        
        hive_options = bigquery.HivePartitioningOptions()
        hive_options.mode = "AUTO"
        hive_options.source_uri_prefix = gcs_prefix

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field='ingest_date'
            ),
            
            hive_partitioning=hive_options,
            
            autodetect=True 
        )

        wildcard_uri = f"{gcs_prefix}*"

        load_job = bq_client.load_table_from_uri(
            wildcard_uri,
            table_ref_str,
            job_config=job_config
        )
        
        log.info(f"BigQuery load job '{load_job.job_id}' started, awaiting completion...")
        load_job.result()
        
        log.info(f"Success: Data loaded to table {table_ref_str}.")
        return True
        
    except Exception as e:
        log.error(f"BigQuery load failed: {e}")
        return False



def run_etl_pipeline(project_id: str, 
                     source_csv_file: str, 
                     gcs_bucket_name: str, 
                     bq_dataset_id: str, 
                     target_table_id: str, 
                     csv_read_params: dict = {}):

    log.info(f"Functional ETL process for '{source_csv_file}' started.")
    
    try:
        ingest_date_str = datetime.utcnow().strftime('%Y-%m-%d')
        target_parquet_file = f"{target_table_id}.parquet"
        gcs_blob_path = (
            f"{target_table_id}/"
            f"ingest_date={ingest_date_str}/"
            f"{target_parquet_file}"
        )
        
        parquet_buffer = extract_transform(source_csv_file, ingest_date_str, csv_read_params)
        if parquet_buffer is None:
            raise Exception("Extract & Transform step failed.")
        
        gcs_uri = load_to_gcs(parquet_buffer, gcs_bucket_name, gcs_blob_path, project_id)
        if gcs_uri is None:
            raise Exception("Upload to GCS step failed.")
        
        success = load_to_bigquery(gcs_uri, project_id, bq_dataset_id, target_table_id)
        if not success:
            raise Exception("BigQuery load step failed.")

        log.info(f"ETL process for '{source_csv_file}' completed successfully.")
        return True
    
    except Exception as e:
        log.error(f"ETL Pipeline for '{source_csv_file}' failed: {e}")
        raise


if __name__ == "__main__":

    log.info("--- Running ETL in LOOP mode ---")

    GCP_PROJECT_ID = "beije-476309" 
    GCS_BUCKET = 'beije-476309-staging-bucket'            
    BQ_DATASET = 'stage'              

    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

    SOURCE_RAW_DIR = os.path.join(PROJECT_ROOT, 'staging_jobs/raw')

    csv_file_list = glob.glob(os.path.join(SOURCE_RAW_DIR, '*.csv'))
    
    if not csv_file_list:
        log.warning(f"No CSV files found in directory: {SOURCE_RAW_DIR}")
    else:
        log.info(f"Found {len(csv_file_list)} CSV files to process.")

    success_count = 0
    failure_count = 0

    for csv_file_path in csv_file_list:
        try:
            file_name = os.path.basename(csv_file_path)
            table_id = os.path.splitext(file_name)[0]
            
            log.info(f"--- [START] Processing: {file_name} (Target Table: {table_id}) ---")
            
            run_etl_pipeline(
                project_id=GCP_PROJECT_ID,
                source_csv_file=csv_file_path,
                gcs_bucket_name=GCS_BUCKET,
                bq_dataset_id=BQ_DATASET,
                target_table_id=table_id,
                csv_read_params={}
            )
            
            log.info(f"--- [SUCCESS] Finished: {file_name} ---")
            success_count += 1
            
        except Exception as e:

            log.error(f"--- [FAILED] Pipeline stopped for: {file_name}. Error: {e} ---")
            failure_count += 1
        
        log.info("-" * 50) 

    log.info("--- ETL Loop Summary ---")
    log.info(f"Total files found: {len(csv_file_list)}")
    log.info(f"Successfully processed: {success_count}")
    log.info(f"Failed to process: {failure_count}")
    log.info("--- ETL Loop Finished ---")
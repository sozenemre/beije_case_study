## Script Overview and Functionality

This script implements a complete ELT (Extract, Load, Transform) workflow for a batch of CSV files, iterating through a specified local directory and processing each file individually. The core principle is to use GCS as a temporary, partitioned staging area before loading data into BigQuery.

### Key Functional Steps:
#### 1- Extract & Transform (extract_transform):

- Reads a local CSV file into a Pandas DataFrame.

- Adds an ingest_date column (set to the UTC execution date), which is crucial for both partitioning and downstream incremental dbt modeling.

- Converts the DataFrame into an in-memory Parquet format using pyarrow. Parquet is an optimized, column-oriented format better suited for cloud storage and BigQuery loading than CSV.

#### 2- Load to GCS (load_to_gcs):

- Uploads the Parquet data buffer to the designated Google Cloud Storage bucket (beije-476309-staging-bucket).

- Uses a Hive-style folder structure (target_table_id/ingest_date=YYYY-MM-DD/file.parquet) to enable BigQuery's auto-detection of partitions.

#### 3- Load to BigQuery (load_to_bigquery):

- Initiates a BigQuery Load Job to move the data from GCS into the final BigQuery table (in the stage dataset).

- Uses WRITE_APPEND to add new data to the existing table.

- Configures Time Partitioning on the ingest_date column for performance.

- Configures Hive Partitioning (hive_partitioning=hive_options) to recognize and load data based on the folder structure created in GCS.

#### 4- Execution Logic (if __name__ == "__main__")
The main execution block sets the GCP context variables (GCP_PROJECT_ID, GCS_BUCKET, BQ_DATASET). It then uses the glob module to find all .csv files in the dedicated staging_jobs/raw directory. It iterates through each found file, executes the full ETL pipeline, and logs the success or failure of each individual file process, providing a final summary of the loop's outcome.
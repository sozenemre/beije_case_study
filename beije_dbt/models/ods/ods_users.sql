{{
  config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "ingest_date",
      "data_type": "date",  
      "granularity": "day"
    },
    cluster_by = ["user_id"],
    tags=["ods"]
  )
}}

SELECT
    _id         as user_id,
    createdAt   as created_at_timestamp,
    ingest_date
    
FROM
    {{ source('stage', 'users') }}

{% if is_incremental() %}

  WHERE ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})

{% endif %}
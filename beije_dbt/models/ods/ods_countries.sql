{{
  config(
    materialized='incremental',
    
    incremental_strategy = 'insert_overwrite',

    partition_by = {
      "field": "ingest_date",
      "data_type": "date",  
      "granularity": "day"
    },
    
    cluster_by = ["country_id"],
    tags=["ods"]
  )
}}

SELECT
    _id         as country_id,
    name        as country_name,
    ingest_date
    
FROM
    {{ source('stage', 'countries') }}

{% if is_incremental() %}

  WHERE ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})

{% endif %}
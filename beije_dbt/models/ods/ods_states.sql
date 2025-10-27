{{
  config(
    materialized='incremental',
    
    incremental_strategy = 'insert_overwrite',

    partition_by = {
      "field": "ingest_date",
      "data_type": "date",  
      "granularity": "day"
    },
    
    cluster_by = ["state_id", "country_id"],
    tags=["ods"]
  )
}}

SELECT
    _id         as state_id,
    name        as state_name,
    _country    as country_id,
    ingest_date
    
FROM
    {{ source('stage', 'states') }}

{% if is_incremental() %}

  WHERE ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})

{% endif %}
{{
  config(
    materialized='incremental',
  
    incremental_strategy = 'insert_overwrite',

    partition_by = {
      "field": "ingest_date",
      "data_type": "date",  
      "granularity": "day"
    },
  
    cluster_by = ["city_id", "state_id", "country_id"],
    tags=["ods"]
  )
}}

SELECT
    _id         as city_id,
    name        as city_name,
    _state      as state_id,
    _country    as country_id,
    ingest_date
    
FROM
    {{ source('stage', 'cities') }}

{% if is_incremental() %}

  WHERE ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})

{% endif %}
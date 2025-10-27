{{
  config(
    materialized='incremental',
    
    incremental_strategy = 'insert_overwrite',

    partition_by = {
      "field": "ingest_date",
      "data_type": "date",  
      "granularity": "day"
    },
    
    cluster_by = ["neighborhood_id", "city_id"],
    tags=["ods"]
  )
}}

SELECT
    _id         as neighborhood_id,
    name        as neighborhood_name,
    _city       as city_id,
    _country    as country_id,
    postalCode  as postal_code,
    ingest_date
    
FROM
    {{ source('stage', 'neighborhoods') }}

{% if is_incremental() %}

  WHERE ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})

{% endif %}
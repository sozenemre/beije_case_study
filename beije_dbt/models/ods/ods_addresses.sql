{{
  config(
    materialized='incremental',
    
    incremental_strategy = 'insert_overwrite',

    partition_by = {
      "field": "ingest_date",
      "data_type": "date",  
      "granularity": "day"
    },
    
    cluster_by = ["address_id", "user_id", "city_id", "state_id"],
    tags=["ods"]
  )
}}

SELECT
    _id             as address_id,
    _user           as user_id,
    _city           as city_id,
    _state          as state_id,
    _country        as country_id,
    _neighborhood   as neighborhood_id,
    invoiceType     as invoice_type,
    ingest_date
    
FROM
    {{ source('stage', 'addresses') }}

{% if is_incremental() %}

  WHERE ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})

{% endif %}
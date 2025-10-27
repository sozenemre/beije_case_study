{{
  config(
    materialized='incremental',

    incremental_strategy = 'insert_overwrite',

    partition_by = {
      "field": "ingest_date",
      "data_type": "date",
      "granularity": "day"
    },

    cluster_by = ["_user", "_id"],
    tags=["ods"]
  )
}}

SELECT
    _id,
    _user,
    createdAt,
    isActive,
    isSkip,
    nextOrderDate,
    startDate,
    totalQuantity,
    ingest_date,
    
    REPLACE(
      REGEXP_REPLACE(products, r"ObjectId\('([^']*)'\)", r"'\1'"),
      "'" , '"'
    ) AS products_json_string
    
FROM
    {{ source('stage', 'subscriptions') }}

{% if is_incremental() %}

  -- 'insert_overwrite' stratejisi için, bu model her çalıştığında
  -- kaynak tablodan sadece hedefte olmayan (daha yeni) 'ingest_date' 
  -- bölümlerini çeker.
  WHERE ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})

{% endif %}
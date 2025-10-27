{{
  config(
    materialized='incremental',
    
    incremental_strategy = 'insert_overwrite',

    partition_by = {
      "field": "ingest_date",
      "data_type": "date",  
      "granularity": "day"
    },
    
    cluster_by = ["shipment_id", "order_id", "user_id"],
    tags=["ods"]
  )
}}

WITH cleaned_json_strings AS (
  SELECT
    *,
    REPLACE(
      REGEXP_REPLACE(details, r"datetime\.datetime\(([^)]+)\)", r"'\1'"),
      "'", '"'
    ) AS details_json_string,
    REPLACE(label, "'", '"') AS label_json_string
    
  FROM
    {{ source('stage', 'shipments') }}
),

extracted_strings AS (
  SELECT
    *,
    JSON_EXTRACT_SCALAR(details_json_string, '$.collectDate') AS collect_date_parts_str,
    JSON_EXTRACT_SCALAR(details_json_string, '$.deliveryDate') AS delivery_date_parts_str,
    JSON_EXTRACT_SCALAR(label_json_string, '$.provider') as provider_name
  FROM
    cleaned_json_strings
),

final_columns_with_arrays AS (
  SELECT
    _id,
    _order,
    _user,
    provider_name,
    ingest_date,
    SPLIT(collect_date_parts_str, ', ') AS collect_parts,
    SPLIT(delivery_date_parts_str, ', ') AS delivery_parts
    
  FROM
    extracted_strings
)

SELECT
    _id         as shipment_id,
    _order      as order_id,
    _user       as user_id,
    provider_name,
    SAFE.DATETIME(
      CAST(collect_parts[SAFE_OFFSET(0)] AS INT64),  -- Yıl
      CAST(collect_parts[SAFE_OFFSET(1)] AS INT64),  -- Ay
      CAST(collect_parts[SAFE_OFFSET(2)] AS INT64),  -- Gün
      CAST(collect_parts[SAFE_OFFSET(3)] AS INT64),  -- Saat
      CAST(collect_parts[SAFE_OFFSET(4)] AS INT64),  -- Dakika
      0                                               -- Saniye
    ) AS collect_datetime,
    
    SAFE.DATETIME(
      CAST(delivery_parts[SAFE_OFFSET(0)] AS INT64), -- Yıl
      CAST(delivery_parts[SAFE_OFFSET(1)] AS INT64), -- Ay
      CAST(delivery_parts[SAFE_OFFSET(2)] AS INT64), -- Gün
      CAST(delivery_parts[SAFE_OFFSET(3)] AS INT64), -- Saat
      CAST(delivery_parts[SAFE_OFFSET(4)] AS INT64), -- Dakika
      0                                               -- Saniye
    ) AS delivery_datetime,

    ingest_date
    
FROM
    final_columns_with_arrays

{% if is_incremental() %}

  WHERE ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})

{% endif %}
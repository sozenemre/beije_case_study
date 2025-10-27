{{
  config(
    materialized='incremental',
    
    incremental_strategy = 'insert_overwrite',

    partition_by = {
      "field": "ingest_date",
      "data_type": "date",
      "granularity": "day"
    },
    
    cluster_by = ["shipment_id", "order_id", "latest_status"],
    tags=["fact"]
  )
}}

SELECT
  shipment_id,
  order_id,
  CASE
    WHEN delivery_datetime IS NOT NULL THEN 'delivered'
    WHEN collect_datetime IS NOT NULL THEN 'in_transit'
    ELSE 'processing'
  END AS latest_status,
  delivery_datetime AS delivered_at,
  provider_name AS carrier,
  ingest_date
  
FROM
  {{ ref('ods_shipments') }}

{% if is_incremental() %}

  WHERE ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})

{% endif %}
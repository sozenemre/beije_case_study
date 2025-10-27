{{
  config(
    materialized='incremental',
    
    incremental_strategy = 'insert_overwrite',

    partition_by = {
      "field": "ingest_date",
      "data_type": "date",
      "granularity": "day"
    },
      
    cluster_by = ["order_date", "order_id", "customer_id"],
    tags=["fact"]
  )
}}

WITH source_with_prices AS (
  SELECT
    order_id,
    user_id AS customer_id,
    DATE(created_at_timestamp) AS order_date,
    status AS payment_status,
    ingest_date,
    CAST(
      JSON_EXTRACT_SCALAR(price_json_string, '$.grossOriginalAmount') 
    AS FLOAT64) AS items_total,
    CAST(
      JSON_EXTRACT_SCALAR(price_json_string, '$.grossPromoDiscountAmount') 
    AS FLOAT64) AS discount_total,
    CAST(
      COALESCE(JSON_EXTRACT_SCALAR(price_json_string, '$.grossShippingAmount'), '0') 
    AS FLOAT64) AS shipping_fee
    
  FROM
    {{ ref('ods_orders') }}

{% if is_incremental() %}

  WHERE ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})

{% endif %}
)

SELECT
  order_id,
  customer_id,
  CAST(NULL AS STRING) AS subscription_id,  
  order_date,
  items_total,
  discount_total,
  shipping_fee,
  CASE
    WHEN payment_status = 'paid'
    THEN (items_total - discount_total + shipping_fee)
    ELSE 0
  END AS net_revenue,
  payment_status,
  ingest_date
  
FROM
  source_with_prices
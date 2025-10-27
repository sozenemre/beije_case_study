{{
  config(
    materialized='incremental',

    incremental_strategy = 'insert_overwrite',

    partition_by = {
      "field": "ingest_date",
      "data_type": "date",  
      "granularity": "day"
    },
    cluster_by = ["order_id", "user_id", "status"],
    tags=["ods"]
  )
}}

SELECT
    _id                 as order_id,
    _user               as user_id,
    _deliveryAddress    as delivery_address_id,
    _invoiceAddress     as invoice_address_id,
    createdAt           as created_at_timestamp,
    oneTimePurchase     as is_one_time_purchase,
    status,
    ingest_date,
    REPLACE(price, "'", '"') AS price_json_string,
    REPLACE(
      REGEXP_REPLACE(subscriptions, r"ObjectId\('([^']*)'\)", r"'\1'"),
      "'", '"'
    ) AS subscriptions_json_string

FROM
    {{ source('stage', 'orders') }}

{% if is_incremental() %}

  WHERE ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})

{% endif %}
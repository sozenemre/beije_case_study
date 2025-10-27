{{
  config(
    materialized='incremental',
    
    incremental_strategy='merge',
    
    unique_key='subscription_id',

    cluster_by=['subscription_id', 'customer_id', 'status'],
    tags=["dim"]
  )
}}

SELECT
    _id         AS subscription_id,
    _user       AS customer_id,
    CASE 
        WHEN isActive = false THEN 'inactive'
        WHEN isSkip = true THEN 'skipped'
        WHEN isActive = true THEN 'active'
        ELSE 'unknown'
    END AS status,
    DATE(CAST(startDate AS TIMESTAMP)) AS start_date,
    CASE
            WHEN isActive = false THEN ingest_date
            ELSE NULL
        END AS end_date,
    ingest_date
    
FROM
    {{ ref('ods_subscriptions') }}

{% if is_incremental() %}

  WHERE ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})

{% endif %}
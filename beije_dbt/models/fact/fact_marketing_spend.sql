{{
  config(
    materialized='table',
       
    cluster_by = ["date", "channel"],
    tags=["fact"]
  )
}}

SELECT
  spend_date    AS date,
  channel,
  spend_amount  AS spend_try,
  CAST(NULL AS INT64) AS clicks
  
FROM
  {{ ref('ods_marketing_spend') }}
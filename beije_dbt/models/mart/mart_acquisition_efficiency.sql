{{
  config(
    materialized='table',
       
    cluster_by=['date'],
    tags=["mart"]
  )
}}

WITH daily_marketing_spend AS (
  SELECT
    spend_date AS date,
    SUM(spend_amount) AS total_spend_try
  FROM
    {{ ref('ods_marketing_spend') }}
  GROUP BY
    1
),

daily_new_subscriptions AS (
  SELECT
    date,
    new_subscribers
  FROM
    {{ ref('mart_subscriptions_daily') }}
  WHERE new_subscribers > 0
)

SELECT
  spend.date,
  COALESCE(spend.total_spend_try, 0) AS daily_marketing_spend_try,
  COALESCE(subs.new_subscribers, 0) AS daily_new_subscribers,
  SAFE_DIVIDE(spend.total_spend_try, subs.new_subscribers) AS daily_cac_try
  
FROM
  daily_marketing_spend AS spend
  LEFT JOIN daily_new_subscriptions AS subs ON spend.date = subs.date
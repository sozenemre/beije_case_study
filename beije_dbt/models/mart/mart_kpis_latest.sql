{{
  config(
    materialized='view',
    tags=["mart"]
  )
}}

WITH mrr_data AS (
  SELECT
    COUNT(t1.subscription_id) AS active_subscribers_count,
    300 AS average_mrr_per_sub,
    (COUNT(t1.subscription_id) * 300) AS total_mrr_try
  FROM
    {{ ref('dim_subscription') }} AS t1
  WHERE
    t1.status = 'active'
),

latest_daily_metrics AS (
  SELECT
    *
  FROM
    {{ ref('mart_subscriptions_daily') }}
  ORDER BY
    date DESC
  LIMIT 1
),

trailing_30_day_cac AS (
  SELECT
    SUM(daily_marketing_spend_try) AS total_30d_spend,
    SUM(daily_new_subscribers) AS total_30d_new_subs
  FROM
    {{ ref('mart_acquisition_efficiency') }}
  WHERE
    date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
)

SELECT
  latest.date AS reporting_date,
  mrr.active_subscribers_count AS active_subscribers,
  mrr.total_mrr_try AS monthly_recurring_revenue_try,
  SAFE_DIVIDE(latest.cancellations, (latest.active_subscribers + latest.cancellations)) * 100 AS daily_churn_rate_pct,
  SAFE_DIVIDE(cac.total_30d_spend, cac.total_30d_new_subs) AS average_30d_cac_try,
  SAFE_DIVIDE(
    SAFE_DIVIDE(cac.total_30d_spend, cac.total_30d_new_subs),
    mrr.average_mrr_per_sub
  ) AS cac_payback_months

FROM
  mrr_data AS mrr
  CROSS JOIN latest_daily_metrics AS latest
  CROSS JOIN trailing_30_day_cac AS cac
LIMIT 1
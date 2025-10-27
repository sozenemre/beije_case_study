{{
  config(
    materialized='table',
       
    cluster_by=['date'],
    tags=["mart"]
  )
}}

WITH date_spine AS (
  SELECT
    calendar_date AS date
  FROM
    UNNEST(
      GENERATE_DATE_ARRAY(
        (SELECT MIN(start_date) FROM {{ ref('dim_subscription') }}),
        CURRENT_DATE()
      )
    ) AS calendar_date
)

SELECT
  spine.date,
  COUNT(DISTINCT sub.subscription_id) AS active_subscribers,
  COUNT(CASE WHEN sub.start_date = spine.date THEN sub.subscription_id END) AS new_subscribers,
  COUNT(CASE WHEN sub.end_date = spine.date THEN sub.subscription_id END) AS cancellations
  
FROM
  date_spine AS spine
  LEFT JOIN {{ ref('dim_subscription') }} AS sub
    ON spine.date >= sub.start_date
    AND (
      spine.date < sub.end_date 
      OR sub.end_date IS NULL
    )
    
GROUP BY
  spine.date
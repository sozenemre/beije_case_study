{{
  config(
    materialized='table',
       
    cluster_by=['date'],
    tags=["mart"]
  )
}}

WITH daily_revenue_and_delivered AS (
  SELECT
    order_date AS date,
    SUM(net_revenue) AS daily_net_revenue,
    SUM(CASE WHEN payment_status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders_count
    
  FROM
    {{ ref('fact_order') }}
  WHERE order_date IS NOT NULL
  
  GROUP BY
    1
)

SELECT
  date,
  
  COALESCE(daily_net_revenue, 0) AS daily_net_revenue_try,
  COALESCE(delivered_orders_count, 0) AS delivered_orders_count
  
FROM
  daily_revenue_and_delivered

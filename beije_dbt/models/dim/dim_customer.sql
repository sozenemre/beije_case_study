{{
  config(
    materialized='table',
    
    cluster_by=['customer_id', 'city_name', 'state_name'],
    tags=["dim"]
  )
}}

WITH customer_orders AS (
  SELECT
    user_id,
    MIN(created_at_timestamp) AS first_order_timestamp,
    MAX(created_at_timestamp) AS last_order_timestamp,
    COUNT(order_id) AS total_orders
  FROM
    {{ ref('ods_orders') }}
  GROUP BY
    1
),

customer_subscriptions AS (
  SELECT
    _user AS user_id,
  LOGICAL_OR(isActive) AS is_active_subscriber
  FROM
    {{ ref('ods_subscriptions') }}
  GROUP BY
    1
),

customer_latest_address AS (
  SELECT
    user_id,
    city_id,
    state_id,
    country_id,
    neighborhood_id,
    ROW_NUMBER() OVER(
      PARTITION BY user_id
      ORDER BY ingest_date DESC, address_id DESC
    ) as address_rank
  FROM
    {{ ref('ods_addresses') }}
),

full_address_details AS (
  SELECT
    addr.user_id,
    city.city_name,
    state.state_name,
    country.country_name,
    n.neighborhood_name
  FROM
    customer_latest_address AS addr
    LEFT JOIN {{ ref('ods_cities') }} AS city ON addr.city_id = city.city_id
    LEFT JOIN {{ ref('ods_states') }} AS state ON addr.state_id = state.state_id
    LEFT JOIN {{ ref('ods_countries') }} AS country ON addr.country_id = country.country_id
    LEFT JOIN {{ ref('ods_neighborhoods') }} AS n ON addr.neighborhood_id = n.neighborhood_id
  WHERE
    addr.address_rank = 1
)

SELECT
  u.user_id AS customer_id,
  DATE(u.created_at_timestamp) AS created_at_date,
  addr.city_name,
  addr.state_name,
  addr.country_name,
  addr.neighborhood_name,
  DATE(ord.first_order_timestamp) AS first_order_date,
  DATE(ord.last_order_timestamp) AS last_order_date,
  COALESCE(ord.total_orders, 0) AS total_orders,
  COALESCE(sub.is_active_subscriber, false) AS is_active_subscriber
  
FROM
  {{ ref('ods_users') }} AS u
  LEFT JOIN customer_orders AS ord ON u.user_id = ord.user_id
  LEFT JOIN customer_subscriptions AS sub ON u.user_id = sub.user_id
  LEFT JOIN full_address_details AS addr ON u.user_id = addr.user_id
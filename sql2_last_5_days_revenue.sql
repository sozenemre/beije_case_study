SELECT
    date,
    daily_net_revenue_try,
    delivered_orders_count
FROM {{ ref('mart_revenue_daily') }}
WHERE
    date >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY)
ORDER BY
    date DESC
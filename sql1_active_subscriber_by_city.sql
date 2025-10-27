WITH Active_Subscriptions_Last_30_Days AS (
    SELECT
        t1.customer_id
    FROM
        {{ ref('dim_subscription') }} AS t1
    WHERE
        t1.status = 'active'
        AND t1.start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
        AND t1.end_date IS NULL
),

Customers_With_City AS (
    SELECT
        t2.customer_id,
        t2.city_name
    FROM
        {{ ref('dim_customer') }} AS t2
)

SELECT
    t3.city_name,
    COUNT(t3.customer_id) AS active_subscriber_count
FROM
    Active_Subscriptions_Last_30_Days AS t1
INNER JOIN 
    Customers_With_City AS t3 
    ON t1.customer_id = t3.customer_id

GROUP BY
    1
ORDER BY
    active_subscriber_count DESC
LIMIT 10
WITH Last_5_Days AS (
    SELECT 
        DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY) AS start_date
),

Daily_Marketing_Spend AS (
    SELECT
        spend_date AS date,
        channel,
        SUM(spend_amount) AS daily_spend_try
    FROM {{ ref('ods_marketing_spend') }}
    WHERE
        spend_date >= (SELECT start_date FROM Last_5_Days)
    GROUP BY
        1, 2
),

Daily_New_Subscriptions AS (
    SELECT
        start_date AS date,
        COUNT(subscription_id) AS daily_new_subscribers
    FROM {{ ref('dim_subscription') }}
    WHERE
        start_date >= (SELECT start_date FROM Last_5_Days)
    GROUP BY
        1
)

SELECT
    t1.date,
    t1.channel,
    t1.daily_spend_try,
    COALESCE(t2.daily_new_subscribers, 0) AS daily_new_subscribers,
    SAFE_DIVIDE(t1.daily_spend_try, COALESCE(t2.daily_new_subscribers, 0)) AS daily_cac_try,
    SAFE_DIVIDE(
        SAFE_DIVIDE(t1.daily_spend_try, COALESCE(t2.daily_new_subscribers, 0)),
        300
    ) AS cac_payback_months

FROM
    Daily_Marketing_Spend AS t1
LEFT JOIN 
    Daily_New_Subscriptions AS t2 ON t1.date = t2.date

ORDER BY
    t1.date DESC, daily_cac_try DESC
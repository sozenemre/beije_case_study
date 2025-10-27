
## 1. Model: `mart_acquisition_efficiency.sql`

This dbt model, typically named mart_acquisition or mart_kpis, is designed as a high-level Marketing Performance Mart focused on calculating the Daily Customer Acquisition Cost (CAC).

The model integrates two primary data sources: daily marketing expenditure (ods_marketing_spend) and calculated new subscriber counts (mart_subscriptions_daily). It uses simple aggregation to determine the total marketing spend per day.

The final output is a clean table that links the daily spend and the resulting new subscribers. The core metric, daily_cac_try, is calculated by dividing the total daily spend by the number of new subscribers, providing a critical efficiency KPI for the marketing team. Since the model uses materialized='table' and joins pre-aggregated data, it is designed for straightforward consumption by downstream BI tools.


## 2. Model: `mart_kpis_latest.sql`
This dbt model, likely named mart_kpis_latest or mart_dashboard_summary, is designed to serve as a high-level Executive Summary View of the company's most critical subscription metrics and financial health indicators.

The model is configured as a view to ensure it always returns real-time data from its underlying mart and dimension tables without storing redundant data. It integrates calculations from three key sources:

- MRR Data: Calculates the Monthly Recurring Revenue (MRR) and total active subscriber count based on the dim_subscription table.

- Latest Daily Metrics: Fetches the most recent daily operational data (such as cancellation counts).

- 30-Day CAC: Summarizes marketing spend and new subscriber counts over the preceding 30 days to derive key efficiency metrics.

The final output presents a single, consolidated row containing core KPIs, including Total MRR, Daily Churn Rate, 30-Day Average CAC, and the crucial CAC Payback Period in months (CAC divided by Average MRR). This view is optimized for immediate use by leadership dashboards.

## 3. Model: `mart_revenue_daily.sql`

This dbt model, typically named mart_revenue_daily, is designed to create a simplified daily summary mart focused on key sales and operational metrics.

The model aggregates data directly from the fact_order table, grouping transactions by order_date. It calculates two primary daily metrics: the daily_net_revenue (the sum of revenue from all orders) and the delivered_orders_count (a count of orders whose final payment status is 'DELIVERED').

The output is materialized as a standard table and is optimized for efficient filtering by clustering the data on the date column. This structure provides analysts and downstream systems with a clean, daily time series view of core financial and delivery performance.

## 4. Model: `mart_subscriptions_daily.sql`

This dbt model, likely named mart_subscriptions_daily, is an essential daily subscription metrics mart designed to calculate key subscriber counts over time.

The model first establishes a complete date spine using BigQuery's GENERATE_DATE_ARRAY function, starting from the earliest subscription start date up to the current day. It then performs a many-to-many join between this date spine and the dim_subscription table. The join condition is crucial: a subscription is considered active for any given date if that date falls between the subscription's start_date and its end_date (or if end_date is NULL).

The final output is aggregated by day, yielding three primary KPIs:

- active_subscribers: The total count of subscriptions considered active on that specific date.

- new_subscribers: The count of subscriptions whose start date matches the specific date.

- cancellations: The count of subscriptions whose end date matches the specific date.

The model is materialized as a performant table and is optimized for time-series queries by clustering the data on the date column. This mart provides the foundation for analyzing churn, growth, and Monthly Recurring Revenue (MRR) trends.
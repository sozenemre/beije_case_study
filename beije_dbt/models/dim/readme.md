## 1. Model: `dim_customer.sql`

This dbt model constructs the dim_customer dimension table by performing a comprehensive data integration process. Its core function is to unify fragmented customer data (users, orders, subscriptions, and addresses) from the raw (ods) layer into a single, cohesive customer record. The model calculates and aggregates both static demographic information (creation date, latest address details) and dynamic behavioral summaries (first/last order dates, total order count, active subscriber status) for each individual customer. Notably, it employs a window function (ROW_NUMBER) to accurately determine a customer's most recent address and joins this information with corresponding geographical details (city, state, country). The final output, materialized as a standard table, provides an enriched and query-ready record for every customer, facilitating analytical queries and customer segmentation efforts.

## 2. Model: `dim_subscription.sql`

This dbt model, typically named dim_subscription, is designed to create a comprehensive and high-performance Customer Subscription Dimension Table.

Its primary function is to transform raw subscription data (ods_subscriptions) into a normalized dimension record that tracks the subscription's lifecycle. It calculates the definitive status (active, inactive, or skipped) using conditional logic on raw flags and determines the subscription's end_date only when it's marked as inactive.

Crucially, the model is configured for incremental merging (materialized='incremental' with incremental_strategy='merge'). This ensures that only new or changed records are processed in each run, making the pipeline highly efficient by avoiding a full rebuild. It uses subscription_id as the unique_key for the merge operation and is optimized for querying using clustering on subscription_id, customer_id, and status.
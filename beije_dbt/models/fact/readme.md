## 1. Model: `fact_Marketing_spend.sql`

This dbt model, typically named fact_marketing_spend, establishes a foundational Fact Table dedicated to tracking and summarizing daily marketing expenditures. Its primary role is to select and map core expenditure data directly from the raw layer (ods_marketing_spend), defining key dimensions like date and channel alongside the spend_try metric. The model is configured as a simple table (implying a full rebuild on each run) and is performance-optimized in BigQuery by clustering the output on date and channel, facilitating faster aggregation and slicing by analysts. It also includes an explicit NULL placeholder for a clicks metric, anticipating future data integration efforts without altering the table structure.

## 2. Model: `fact_order.sql`

This dbt model, likely named fact_order, is designed to create a high-performance fact table dedicated to tracking order transactions and associated revenue metrics. It processes raw order data (ods_orders), extracting key figures like item total, discount, and shipping fees from embedded JSON fields. The configuration is heavily optimized for BigQuery, using daily partitioning on the ingest_date field and clustering on order_date, order_id, and customer_id for enhanced query performance. The model manages incremental loading using an insert_overwrite strategy, efficiently ensuring that only new partitions of data are added or completely rewritten. Finally, the model calculates the essential net_revenue metric based on the payment status of the order, providing a clean, analytical-ready output.

## 3. Model: `fact_shipment.sql`

This dbt model, designed as a Fact Table for shipments (likely named fact_shipment), focuses on tracking the status and key timestamps of every shipment associated with an order.

The model is heavily optimized for performance on BigQuery, utilizing an insert_overwrite incremental strategy: it updates specific partitions of the data rather than rebuilding the entire table. It achieves this by partitioning the data daily on the ingest_date field. Furthermore, the table is clustered by shipment_id, order_id, and the calculated latest_status, which ensures faster query execution on common analytical filters.

The core transformation logic calculates the latest_status by evaluating the presence of key timestamps (delivery_datetime or collect_datetime), categorizing the shipment as 'delivered', 'in_transit', or 'processing'. This structure provides analysts with an efficient, granular view of the shipping lifecycle.
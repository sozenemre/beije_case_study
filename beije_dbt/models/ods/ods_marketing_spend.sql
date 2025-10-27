{{
  config(
    materialized='table',
       
    cluster_by = ["spend_date", "channel"],
    tags=["ods"]
  )
}}

WITH step1_row_to_json AS (
  SELECT
    Channel,
    TO_JSON_STRING(t) AS json_row
  FROM
    {{ source('stage', 'MarketingSpend') }} AS t
),

step2_unnest_keys AS (
  SELECT
    Channel,
    json_row,
    key
  FROM
    step1_row_to_json,
    UNNEST(REGEXP_EXTRACT_ALL(json_row, r'"([^"]+)":')) AS key
)

SELECT
  Channel AS channel,
  PARSE_DATE('%Y_%m_%d', LTRIM(key, '_')) AS spend_date,
  CAST(
    REPLACE(REGEXP_EXTRACT(json_row, CONCAT('"', key, '":"([^"]*)"')), ',', '')
  AS FLOAT64) AS spend_amount

FROM
  step2_unnest_keys
WHERE
  key != 'Channel'
  AND key != 'ingest_date'
  AND REGEXP_EXTRACT(json_row, CONCAT('"', key, '":"([^"]*)"')) IS NOT NULL

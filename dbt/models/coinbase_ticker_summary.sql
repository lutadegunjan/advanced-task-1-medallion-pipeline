{{ config(materialized='table', schema='gold') }}

SELECT
    product_id,
    count() AS ticker_count,
    avg(price) AS avg_price,
    min(price) AS min_price,
    max(price) AS max_price,
    avg(volume_24_h) AS avg_volume_24_h,
    max(event_timestamp) AS latest_event
FROM {{ ref('coinbase_ticker') }}
GROUP BY product_id

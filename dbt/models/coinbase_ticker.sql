{{ config(materialized='table', schema='silver') }}

SELECT
    channel,
    parsed_event_timestamp AS event_timestamp,
    toUInt64OrZero(raw_sequence_num) AS sequence_num,
    JSONExtract(raw_ticker_type, 'Array(String)') AS ticker_type,
    product_id,
    toFloat64OrZero(raw_price) AS price,
    toFloat64OrZero(raw_volume_24_h) AS volume_24_h,
    toFloat64OrZero(raw_low_24_h) AS low_24_h,
    toFloat64OrZero(raw_high_24_h) AS high_24_h,
    toFloat64OrZero(raw_low_52_w) AS low_52_w,
    toFloat64OrZero(raw_high_52_w) AS high_52_w,
    toFloat64OrZero(raw_price_percent_chg_24_h) AS price_percent_chg_24_h,
    toFloat64OrZero(raw_best_bid) AS best_bid,
    toFloat64OrZero(raw_best_bid_quantity) AS best_bid_quantity,
    toFloat64OrZero(raw_best_ask) AS best_ask,
    toFloat64OrZero(raw_best_ask_quantity) AS best_ask_quantity
FROM
(
    SELECT
        channel,
        parseDateTime64BestEffortOrNull(raw_event_timestamp, 3, 'UTC') AS parsed_event_timestamp,
        raw_sequence_num,
        raw_ticker_type,
        product_id,
        raw_price,
        raw_volume_24_h,
        raw_low_24_h,
        raw_high_24_h,
        raw_low_52_w,
        raw_high_52_w,
        raw_price_percent_chg_24_h,
        raw_best_bid,
        raw_best_bid_quantity,
        raw_best_ask,
        raw_best_ask_quantity
    FROM
    (
        SELECT
            channel,
            event_timestamp AS raw_event_timestamp,
            sequence_num AS raw_sequence_num,
            ticker_type AS raw_ticker_type,
            product_id,
            price AS raw_price,
            volume_24_h AS raw_volume_24_h,
            low_24_h AS raw_low_24_h,
            high_24_h AS raw_high_24_h,
            low_52_w AS raw_low_52_w,
            high_52_w AS raw_high_52_w,
            price_percent_chg_24_h AS raw_price_percent_chg_24_h,
            best_bid AS raw_best_bid,
            best_bid_quantity AS raw_best_bid_quantity,
            best_ask AS raw_best_ask,
            best_ask_quantity AS raw_best_ask_quantity
        FROM bronze.coinbase_ticker
        WHERE channel = 'ticker'
          AND product_id != ''
    )
)
WHERE parsed_event_timestamp IS NOT NULL
  AND toFloat64OrNull(raw_price) IS NOT NULL

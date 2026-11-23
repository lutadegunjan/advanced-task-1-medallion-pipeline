CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.email_thread_details
(
    thread_id String,
    subject String,
    raw_timestamp String,
    sender String,
    recipient String,
    body String
)
ENGINE = MergeTree
ORDER BY (thread_id, raw_timestamp);

CREATE TABLE IF NOT EXISTS bronze.coinbase_ticker
(
    channel String,
    event_timestamp String,
    sequence_num String,
    ticker_type String,
    product_id String,
    price String,
    volume_24_h String,
    low_24_h String,
    high_24_h String,
    low_52_w String,
    high_52_w String,
    price_percent_chg_24_h String,
    best_bid String,
    best_bid_quantity String,
    best_ask String,
    best_ask_quantity String
)
ENGINE = MergeTree
ORDER BY (product_id, event_timestamp);

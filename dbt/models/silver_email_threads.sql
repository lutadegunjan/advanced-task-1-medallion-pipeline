{{ config(materialized='table', schema='silver') }}

SELECT
    thread_id,
    subject,
    parseDateTimeBestEffortOrNull(raw_timestamp) AS email_timestamp,
    lowerUTF8(trim(sender)) AS sender,
    lowerUTF8(trim(recipient)) AS recipient,
    body
FROM bronze.email_thread_details
WHERE thread_id != ''

{{ config(materialized='table', schema='gold') }}

SELECT
    thread_id,
    any(subject) AS subject,
    count() AS email_count,
    uniqExact(sender) AS unique_senders,
    min(email_timestamp) AS first_email,
    max(email_timestamp) AS latest_email
FROM {{ ref('silver_email_threads') }}
GROUP BY thread_id

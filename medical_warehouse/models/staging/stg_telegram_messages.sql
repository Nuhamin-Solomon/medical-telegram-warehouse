{{ config(materialized='view') }}

SELECT
    message_id,
    channel_name,
    message_date,
    message_text,
    views,
    forwards,
    has_media,
    image_path
FROM raw.telegram_messages

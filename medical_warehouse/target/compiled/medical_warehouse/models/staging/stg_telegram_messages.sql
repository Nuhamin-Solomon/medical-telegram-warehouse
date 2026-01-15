-- models/staging/stg_telegram_messages.sql
-- This staging model cleans and standardizes raw Telegram messages for downstream analytics

with raw as (
    select *
    from "medical_dw"."analytics"."telegram_messages"  -- Reference your raw table
),

cleaned as (
    select
        message_id,                                  -- Unique identifier for each message
        channel_name,                                -- Telegram channel name
        message_date::timestamp as message_date,    -- Convert to timestamp
        message_text,                                -- Full text of the message
        has_media,                                   -- Boolean flag if message contains media
        image_path,                                  -- Path to downloaded image
        views::int as views,                         -- Cast views to integer
        forwards::int as forwards,                   -- Cast forwards to integer
        length(message_text) as message_length,     -- Derived column: message length
        case when has_media then 1 else 0 end as has_image  -- Flag for messages with images
    from raw
    where message_id is not null                     -- Remove rows with null IDs
)

select *
from cleaned
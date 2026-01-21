{{ config(materialized='table') }}

-- This model selects raw YOLO detection results from your ingestion table
select
    message_id,
    image_path,
    detected_objects,
    detected_classes
from {{ source('raw', 'telegram_messages') }}  -- replace 'telegram_messages' with your raw table if needed

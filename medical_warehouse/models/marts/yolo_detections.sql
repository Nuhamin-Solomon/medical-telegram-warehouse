{{ config(materialized='table') }}

-- Flatten YOLO detection results and classify product categories
select
    message_id,
    image_path,
    jsonb_array_elements(detected_objects) as object,
    jsonb_array_elements(detected_classes) as category,
    case
        when jsonb_array_elements(detected_classes) in ('pill','capsule') then 'medicine'
        when jsonb_array_elements(detected_classes) in ('syrup') then 'liquid'
        else 'other'
    end as product_category
from {{ ref('raw_yolo_results') }}

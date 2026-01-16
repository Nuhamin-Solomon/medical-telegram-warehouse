with detections as (

    select
        message_id::bigint,
        detected_object,
        confidence
    from public.yolo_detections

),

messages as (

    select *
    from {{ ref('fct_messages') }}

)

select
    m.message_id,
    m.channel_key,
    m.date_key,
    d.detected_object,
    d.confidence
from detections d
join messages m
    on d.message_id = m.message_id

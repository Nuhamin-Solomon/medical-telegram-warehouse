with raw as (

    select
        message_id,
        channel_name,
        message_date::timestamp as message_date,
        message_text,
        views::int as view_count,
        forwards::int as forward_count,
        case when has_media then true else false end as has_image,
        image_path,
        length(message_text) as message_length
    from {{ source('raw', 'telegram_messages') }}
    where message_text is not null

)

select * from raw

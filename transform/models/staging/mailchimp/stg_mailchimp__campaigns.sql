with source as (
    select * from {{ source('matt_staging', 'mailchimp_campaigns_raw') }}
),

parsed as (
    select
        json_data:campaign_id::varchar as campaign_id,
        json_data:list_id::varchar as list_id,
        json_data:type::varchar as type,
        json_data:status::varchar as status,
        json_data:subject_line::varchar as subject_line,
        json_data:from_name::varchar as from_name,
        json_data:from_email::varchar as from_email,
        json_data:send_time::timestamp_tz as send_time,
        json_data:emails_sent::integer as emails_sent
    from source
)

select * from parsed

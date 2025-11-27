with source as (
    select * from {{ source('matt_staging', 'mailchimp_subscribers_raw') }}
),

parsed as (
    select
        json_data:subscriber_hash::varchar as subscriber_hash,
        json_data:list_id::varchar as list_id,
        lower(json_data:email_address::varchar) as email_address,
        json_data:status::varchar as status,
        try_to_timestamp_tz(json_data:timestamp_opt::varchar) as timestamp_opt,
        try_to_timestamp_tz(json_data:last_changed::varchar) as last_changed,
        parse_json(json_data:merge_fields::varchar) as merge_fields,
        parse_json(json_data:merge_fields::varchar):FNAME::varchar as first_name,
        parse_json(json_data:merge_fields::varchar):LNAME::varchar as last_name,
        parse_json(json_data:merge_fields::varchar):COMPANY::varchar as company
    from source
)

select * from parsed

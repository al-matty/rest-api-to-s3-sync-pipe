with source as (
    select * from {{ source('matt_staging', 'mailchimp_clicks_raw') }}
),

parsed as (
    select
        {{ dbt_utils.generate_surrogate_key(['json_data:campaign_id', 'json_data:subscriber_hash', 'json_data:url_id']) }} as click_id,
        json_data:campaign_id::varchar as campaign_id,
        json_data:subscriber_hash::varchar as subscriber_hash,
        lower(json_data:email_address::varchar) as email_address,
        json_data:url_id::varchar as url_id,
        json_data:url::varchar as url,
        json_data:click_count::integer as click_count
    from source
)

select * from parsed

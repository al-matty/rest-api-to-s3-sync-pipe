with source as (
    select * from {{ source('matt_staging', 'mailchimp_campaign_reports_raw') }}
),

parsed as (
    select
        json_data:campaign_id::varchar as campaign_id,
        json_data:emails_sent::integer as emails_sent,
        json_data:unique_opens::integer as unique_opens,
        json_data:opens_total::integer as opens_total,
        json_data:open_rate::float as open_rate,
        json_data:unique_clicks::integer as unique_clicks,
        json_data:clicks_total::integer as clicks_total,
        json_data:click_rate::float as click_rate,
        json_data:unsubscribed::integer as unsubscribed
    from source
)

select * from parsed

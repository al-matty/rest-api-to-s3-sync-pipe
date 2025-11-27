with campaigns as (
    select * from {{ ref('stg_mailchimp__campaigns') }}
),

reports as (
    select * from {{ ref('stg_mailchimp__campaign_reports') }}
),

campaigns_with_reports as (
    select
        c.campaign_id,
        c.list_id,
        c.type,
        c.status,
        c.subject_line,
        c.from_name,
        c.from_email,
        c.send_time,
        c.emails_sent,
        r.unique_opens,
        r.opens_total,
        r.open_rate,
        r.unique_clicks,
        r.clicks_total,
        r.click_rate,
        r.unsubscribed
    from campaigns c
    left join reports r on c.campaign_id = r.campaign_id
)

select * from campaigns_with_reports

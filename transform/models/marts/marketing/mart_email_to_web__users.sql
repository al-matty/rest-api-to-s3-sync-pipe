with subscribers as (
    select * from {{ ref('int_mailchimp__subscribers') }}
),

amplitude_users as (
    select * from {{ ref('int_amplitude__users') }}
),

clicks as (
    select
        subscriber_hash,
        count(distinct campaign_id) as campaigns_clicked,
        sum(click_count) as total_clicks,
        min(campaign_send_time) as first_click_campaign_date,
        max(campaign_send_time) as last_click_campaign_date
    from {{ ref('int_mailchimp__clicks') }}
    group by subscriber_hash
)

select
    -- Subscriber data
    s.subscriber_hash,
    s.email_address,
    s.full_name,
    s.company as mailchimp_company,
    s.is_active as is_subscribed,
    s.timestamp_opt as subscribed_at,

    -- Email metrics
    coalesce(c.campaigns_clicked, 0) as campaigns_clicked,
    coalesce(c.total_clicks, 0) as total_email_clicks,
    c.first_click_campaign_date,
    c.last_click_campaign_date,

    -- Amplitude: Website activity 
    case when au.amplitude_id is not null then true else false end as has_website_activity,
    au.amplitude_id,
    au.last_seen as last_website_visit,
    au.company as amplitude_company,

    -- 'Funnel'
    case
        when c.subscriber_hash is null then 'no_clicks'
        when au.amplitude_id is null then 'clicked_no_website'
        else 'clicked_and_visited'
    end as funnel_stage

from subscribers s
left join clicks c on s.subscriber_hash = c.subscriber_hash
left join amplitude_users au on lower(s.email_address) = lower(au.user_id)

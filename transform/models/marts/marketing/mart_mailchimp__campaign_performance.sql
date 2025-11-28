with campaigns as (
    select * from {{ ref('int_mailchimp__campaigns') }}
),

clicks as (
    select * from {{ ref('int_mailchimp__clicks') }}
),

click_stats as (
    select
        campaign_id,
        count(distinct subscriber_hash) as unique_clickers,
        count(*) as total_click_records,
        sum(click_count) as total_clicks,
        -- Multiple clickers: users who clicked the same URL more than once
        count(case when click_count > 1 then 1 end) as multi_click_records,
        count(distinct case when click_count > 1 then subscriber_hash end) as multi_clickers
    from clicks
    group by campaign_id
)

select
    c.campaign_id,
    c.subject_line,
    c.send_time,
    c.status,
    c.emails_sent,

    -- Click rates (from campaign reports)
    c.unique_clicks,
    c.clicks_total,
    c.click_rate,

    -- Multiple clickers analysis
    cs.multi_clickers,
    cs.total_clicks,
    round(cs.multi_clickers / nullif(cs.unique_clickers, 0) * 100, 2) as multi_click_pct,

    -- Open metrics for topic analysis
    c.unique_opens,
    c.open_rate

from campaigns c
left join click_stats cs on c.campaign_id = cs.campaign_id
where c.status = 'sent'

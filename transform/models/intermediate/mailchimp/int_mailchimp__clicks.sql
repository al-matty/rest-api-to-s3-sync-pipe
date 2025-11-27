with clicks as (
    select * from {{ ref('stg_mailchimp__clicks') }}
),

campaigns as (
    select * from {{ ref('int_mailchimp__campaigns') }}
),

subscribers as (
    select * from {{ ref('int_mailchimp__subscribers') }}
),

enriched as (
    select
        -- Click fields
        cl.click_id,
        cl.campaign_id,
        cl.subscriber_hash,
        cl.email_address,
        cl.url_id,
        cl.url,
        cl.click_count,

        -- Campaign context
        ca.subject_line as campaign_subject,
        ca.send_time as campaign_send_time,
        ca.status as campaign_status,

        -- Subscriber context
        su.full_name as subscriber_name,
        su.company as subscriber_company,
        su.is_active as subscriber_is_active,

        -- Derived fields
        parse_url(cl.url):host::varchar as url_domain
    from clicks cl
    left join campaigns ca on cl.campaign_id = ca.campaign_id
    left join subscribers su on cl.subscriber_hash = su.subscriber_hash
)

select * from enriched

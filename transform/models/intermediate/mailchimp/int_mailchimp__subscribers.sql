with subscribers as (
    select * from {{ ref('stg_mailchimp__subscribers') }}
),

enriched as (
    select
        subscriber_hash,
        list_id,
        email_address,
        status,
        timestamp_opt,
        last_changed,
        merge_fields,
        first_name,
        last_name,
        company,

        -- Derived fields
        split_part(email_address, '@', 2) as email_domain,
        (status = 'subscribed') as is_active,
        nullif(trim(coalesce(first_name, '') || ' ' || coalesce(last_name, '')), '') as full_name,
        datediff('day', timestamp_opt, current_timestamp()) as subscription_days
    from subscribers
)

select * from enriched

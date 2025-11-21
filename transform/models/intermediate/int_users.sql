with staging as (
    select * from {{ ref('stg_amplitude_events') }}
),

ip_addresses as (
    select * from {{ ref('int_ip_addresses') }}
),

ranked_users as (
    select
        amplitude_id,
        user_id,
        device_id,
        ip_address,
        event_time,
        row_number() over (
            partition by amplitude_id
            order by event_time desc
        ) as rn
    from staging
    where amplitude_id is not null
),

latest_per_user as (
    select
        r.amplitude_id,
        r.user_id,
        r.device_id,
        i.ip_address_id,
        null as company,  -- Enriched by int_companies
        r.event_time as last_seen
    from ranked_users r
    left join ip_addresses i on r.ip_address = i.ip_address
    where r.rn = 1
)

select * from latest_per_user

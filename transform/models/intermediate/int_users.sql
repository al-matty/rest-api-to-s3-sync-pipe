with staging as (
    select * from {{ ref('stg_amplitude_events') }}
),

ip_addresses as (
    select * from {{ ref('int_ip_addresses') }}
),

companies as (
    select * from {{ ref('int_companies') }}
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

user_company_match as (
    select
        r.amplitude_id,
        r.user_id,
        r.device_id,
        r.ip_address,
        r.event_time,
        -- Extract company from user's email domain
        case
            when r.user_id is not null and contains(r.user_id, '@')
            then lower(split_part(r.user_id, '@', 2))
            else null
        end as user_email_domain
    from ranked_users r
    where r.rn = 1
),

latest_per_user as (
    select
        u.amplitude_id,
        u.user_id,
        u.device_id,
        i.ip_address_id,
        -- Prefer company matching user's email domain, otherwise pick first alphabetically
        coalesce(
            max(case when c.company = u.user_email_domain then c.company end),
            min(c.company)
        ) as company,
        u.event_time as last_seen
    from user_company_match u
    left join ip_addresses i on u.ip_address = i.ip_address
    left join companies c on c.ip_address = i.ip_address
    group by
        u.amplitude_id,
        u.user_id,
        u.device_id,
        i.ip_address_id,
        u.event_time
)

--select * from latest_per_user
select * from latest_per_user where company is not null
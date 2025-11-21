with staging as (
    select * from {{ ref('stg_amplitude_events') }}
),

ip_addresses as (
    select * from {{ ref('int_ip_addresses') }}
),

user_companies as (
    select distinct
        lower(split_part(user_id, '@', 2)) as company,
        ip_address
    from staging
    where user_id is not null
        and contains(user_id, '@')
        and lower(split_part(user_id, '@', 2)) not in (
            'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
            'aol.com', 'icloud.com', 'mail.com', 'protonmail.com',
            'live.com', 'msn.com', 'ymail.com', 'googlemail.com',
            'me.com', 'mac.com', 'comcast.net', 'verizon.net'
        )
),

company_extraction as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['c.company', 'i.ip_address']) }} as company_id,
        c.company,
        i.ip_address
    from user_companies c
    join ip_addresses i on c.ip_address = i.ip_address
)

select * from company_extraction

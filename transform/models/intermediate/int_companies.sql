with users as (
    select * from {{ ref('int_users') }}
),

ip_addresses as (
    select * from {{ ref('int_ip_addresses') }}
),

company_extraction as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['company', 'i.ip_address']) }} as company_id,
        company,
        i.ip_address
    from (
        select
            lower(split_part(u.user_id, '@', 2)) as company,
            u.ip_address_id
        from users u
        where u.user_id is not null
            and contains(u.user_id, '@')
            and lower(split_part(u.user_id, '@', 2)) not in (
                'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
                'aol.com', 'icloud.com', 'mail.com', 'protonmail.com',
                'live.com', 'msn.com', 'ymail.com', 'googlemail.com',
                'me.com', 'mac.com', 'comcast.net', 'verizon.net'
            )
    ) companies
    join ip_addresses i on companies.ip_address_id = i.ip_address_id
)

select * from company_extraction

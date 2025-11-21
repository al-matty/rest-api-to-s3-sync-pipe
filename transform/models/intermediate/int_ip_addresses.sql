with staging as (
    select * from {{ ref('stg_amplitude_events') }}
),

distinct_ips as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['ip_address']) }} as ip_address_id,
        ip_address
    from staging
    where ip_address is not null
)

select * from distinct_ips

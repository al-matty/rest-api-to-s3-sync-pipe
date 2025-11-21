{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='sync_all_columns'
    )
}}

with staging as (
    select * from {{ ref('stg_amplitude_events') }}
    {% if is_incremental() %}
    where event_time > (select max(event_time) from {{ this }})
    {% endif %}
),

users as (
    select * from {{ ref('int_users') }}
),

deduplicated as (
    select *,
        row_number() over (
            partition by uuid
            order by event_time desc
        ) as rn
    from staging
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['d.uuid']) }} as event_id,
        d.event_id_raw,
        {{ dbt_utils.generate_surrogate_key(['d.session_id_raw', 'd.amplitude_id']) }} as session_id,
        d.session_id_raw,
        d.amplitude_id,
        {{ dbt_utils.generate_surrogate_key(['d.ip_address']) }} as ip_address_id,
        d.event_time,
        d.event_type,
        {{ dbt_utils.generate_surrogate_key(['to_json(d.user_properties)']) }} as user_properties_id,
        case
            when d.event_properties is not null
                and to_json(d.event_properties) != '{}'
            then {{ dbt_utils.generate_surrogate_key(['to_json(d.event_properties)']) }}
            else null
        end as event_properties_id,
        u.company as company_name,
        d.uuid
    from deduplicated d
    left join users u on d.amplitude_id = u.amplitude_id
    where d.rn = 1
)

select * from final
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
        md5(uuid) as event_id,
        event_id_raw,
        md5(concat(session_id_raw, '|', amplitude_id)) as session_id,
        session_id_raw,
        amplitude_id,
        md5(ip_address) as ip_address_id,
        event_time,
        event_type,
        case 
            when event_properties is not null 
                and to_json(event_properties) != '{}'
            then md5(to_json(event_properties))
            else null 
        end as event_properties_id,
        uuid
    from deduplicated
    where rn = 1
)

select * from final
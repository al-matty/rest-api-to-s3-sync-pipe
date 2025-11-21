{{
    config(
        materialized='incremental',
        unique_key='session_id',
        on_schema_change='sync_all_columns'
    )
}}

with staging as (
    select * from {{ ref('stg_amplitude_events') }}
    {% if is_incremental() %}
    where event_time > (select max(session_end) from {{ this }})
    {% endif %}
),

session_aggregates as (
    select
        {{ dbt_utils.generate_surrogate_key(['session_id_raw', 'amplitude_id']) }} as session_id,
        session_id_raw,
        amplitude_id,
        any_value(user_id) as user_id,
        any_value(device_id) as device_id,
        min(event_time) as session_start,
        max(event_time) as session_end,
        count(*) as event_count,
        listagg(
            coalesce(event_properties:"[Amplitude] Page Path"::varchar, ''),
            ' > '
        ) within group (order by event_time) as user_journey,
        {{ dbt_utils.generate_surrogate_key([
            'coalesce(any_value(city), \'\')',
            'coalesce(any_value(region), \'\')',
            'coalesce(any_value(country), \'\')'
        ]) }} as location_id
    from staging
    where session_id_raw is not null
    group by session_id_raw, amplitude_id
)

select * from session_aggregates

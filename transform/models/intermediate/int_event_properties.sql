with staging as (
    select * from {{ ref('stg_amplitude_events') }}
),

distinct_props as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['to_json(event_properties)']) }} as event_properties_id,
        event_properties as properties_json,
        left(event_properties:"[Amplitude] Page URL"::varchar, 1000) as url,
        left(event_properties:referrer::varchar, 1000) as referrer,
        left(event_properties:referring_domain::varchar, 255) as referring_domain,
        left(event_properties:"[Amplitude] Element Text"::varchar, 500) as element_text,
        left(event_properties:"[Amplitude] Page Title"::varchar, 500) as page_title
    from staging
    where event_properties is not null
        and to_json(event_properties) != '{}'
)

select * from distinct_props

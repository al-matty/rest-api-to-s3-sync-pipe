with source as (
    select * from {{ source('matt_staging', 'amplitude_events_raw_python') }}
),

parsed as (
    select
        json_data:"$insert_id"::varchar as insert_id,
        json_data:amplitude_id::number as amplitude_id,
        json_data:app::number as app,
        json_data:city::varchar as city,
        json_data:client_event_time::timestamp_ntz as client_event_time,
        json_data:client_upload_time::timestamp_ntz as client_upload_time,
        json_data:country::varchar as country,
        json_data:device_family::varchar as device_family,
        json_data:device_id::varchar as device_id,
        json_data:device_type::varchar as device_type,
        json_data:dma::varchar as dma,
        json_data:event_id::number as event_id_raw,
        json_data:event_properties as event_properties,
        json_data:event_time::timestamp_ntz as event_time,
        json_data:event_type::varchar as event_type,
        json_data:ip_address::varchar as ip_address,
        json_data:language::varchar as language,
        json_data:library::varchar as library,
        json_data:os_name::varchar as os_name,
        json_data:os_version::varchar as os_version,
        json_data:platform::varchar as platform,
        json_data:processed_time::timestamp_ntz as processed_time,
        json_data:region::varchar as region,
        json_data:server_received_time::timestamp_ntz as server_received_time,
        json_data:server_upload_time::timestamp_ntz as server_upload_time,
        json_data:session_id::number as session_id_raw,
        json_data:user_id::varchar as user_id,
        json_data:user_properties as user_properties,
        json_data:uuid::varchar as uuid
    from source
)

select * from parsed
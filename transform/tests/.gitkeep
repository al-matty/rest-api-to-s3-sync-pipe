-- Events should have unique UUIDs
select
    uuid,
    count(*) as cnt
from {{ ref('int_events') }}
group by uuid
having count(*) > 1
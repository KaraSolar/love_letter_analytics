-- Boundary points (edges) are inside and thus reliability must be False
-- We check edges independently to ensure inclusive logic
with boundary as (
  select *
  from {{ ref('love_letter_telemetry') }}
  where (longitude in (-0.2, 0.8) and latitude between -0.2 and 0.8)
     or (latitude in (-0.2, 0.8) and longitude between -0.2 and 0.8)
)
select count(*) as cnt
from boundary
where is_gps_reading_reliable not in (false, 0);
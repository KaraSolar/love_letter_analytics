-- For any row outside the bounding box, reliability must be True
select count(*) as cnt
from {{ ref('love_letter_telemetry') }}
where not (longitude between -0.2 and 0.8 and latitude between -0.2 and 0.8)
  and is_gps_reading_reliable not in (true, 1);
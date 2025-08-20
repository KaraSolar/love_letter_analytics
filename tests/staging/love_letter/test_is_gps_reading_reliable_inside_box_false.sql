-- For rows strictly inside the bounding box, reliability must be False
-- Box (inclusive): longitude between -0.2 and 0.8 AND latitude between -0.2 and 0.8
select count(*) as cnt
from {{ ref('love_letter_telemetry') }}
where longitude between -0.2 and 0.8
  and latitude between -0.2 and 0.8
  and is_gps_reading_reliable not in (false, 0);
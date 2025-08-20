-- If latitude or longitude is NULL, CASE ELSE branch makes reliability True
select count(*) as cnt
from {{ ref('love_letter_telemetry') }}
where (latitude is null or longitude is null)
  and is_gps_reading_reliable not in (true, 1);
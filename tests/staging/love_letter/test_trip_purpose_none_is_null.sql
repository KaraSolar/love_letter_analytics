-- Fails if any row still contains 'None' literal after transformation
select count(*) as cnt
from {{ ref('love_letter_telemetry') }}
where trip_purpose = 'None';
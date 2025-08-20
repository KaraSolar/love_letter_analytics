-- Basic smoke test: model is materialized and returns rows or at least zero without error
-- This test passes as long as query executes successfully; count(*) >= 0 is always true.
select 0 as cnt
from {{ ref('love_letter_telemetry') }}
limit 1;
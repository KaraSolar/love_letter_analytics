This project uses dbt generic tests via schema.yml (framework: dbt, with dbt-utils and optionally dbt-expectations).
The tests added for models/intermediate/love_letter cover the following:
- Key constraints: trip_id (unique and not null), boat (not null), and trip_number (not null).
- Numerical invariants: durations, distances, and speeds are non-negative; power metrics have expected signs.
- Cross-field integrity: avg_speed between min/max; trip_end >= trip_start.
- Array bounds: first_hundred_points and last_hundred_points have length <= 100.

Run with:
  dbt test --select love_letter
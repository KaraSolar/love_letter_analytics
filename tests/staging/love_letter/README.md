These tests validate the transformation logic for the love_letter telemetry model.

Frameworks:
- dbt Core tests
- Schema/YAML generic tests
- SQL data tests
- If present in packages.yml, dbt_utils-based tests are also included

Assumptions:
- The model under test is named `love_letter_telemetry`. If the actual model name differs,
  update the `ref` targets in the SQL tests and the `name` in `models/staging/love_letter/schema.yml`

Running the tests:
```bash
dbt build --select love_letter_telemetry
dbt test --select love_letter_telemetry
```

Focus areas:
- Convert `trip_purpose` value `'None'` to `NULL`
- `is_gps_reading_reliable` logic: in-box (`[-0.2, 0.8]` inclusive) vs. out-of-box positions
- Numeric and GPS domain sanity checks
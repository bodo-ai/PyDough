WITH simple_range AS (
  SELECT
    SEQ4() AS value
  FROM TABLE(GENERATOR(ROWCOUNT => 10))
)
SELECT
  value
FROM simple_range

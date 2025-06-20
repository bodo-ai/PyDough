WITH _s3 AS (
  SELECT
    a
  FROM table
)
SELECT
  _s0.a
FROM table AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0.a = _s3.a

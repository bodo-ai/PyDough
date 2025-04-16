WITH _s0 AS (
  SELECT
    a
  FROM table
)
SELECT
  table.b AS d
FROM _s0 AS _s0
JOIN table AS table
  ON _s0.a = table.a
LEFT JOIN _s0 AS _s3
  ON _s0.a = _s3.a

WITH _t0 AS (
  SELECT
    a,
    b
  FROM table
  ORDER BY
    a
  LIMIT 5
)
SELECT
  a,
  b
FROM _t0
ORDER BY
  b DESC
LIMIT 2

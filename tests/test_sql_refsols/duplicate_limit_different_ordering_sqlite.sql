WITH _t0 AS (
  SELECT
    a,
    b
  FROM table
  ORDER BY
    1
  LIMIT 5
)
SELECT
  a,
  b
FROM _t0
ORDER BY
  2 DESC
LIMIT 2

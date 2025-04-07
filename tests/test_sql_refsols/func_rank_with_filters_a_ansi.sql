WITH _t0 AS (
  SELECT
    RANK() OVER (ORDER BY a) AS r,
    a,
    b
  FROM table
  WHERE
    b = 0
)
SELECT
  a,
  b,
  r
FROM _t0
WHERE
  r >= 3

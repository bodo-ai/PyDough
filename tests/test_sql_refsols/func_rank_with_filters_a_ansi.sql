WITH _t1 AS (
  SELECT
    RANK() OVER (ORDER BY a) AS r,
    a,
    b
  FROM table
)
SELECT
  a,
  b,
  r
FROM _t1
WHERE
  b = 0 AND r >= 3

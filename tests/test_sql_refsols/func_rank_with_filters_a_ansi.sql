WITH _t0 AS (
  SELECT
    RANK() OVER (ORDER BY a) AS r,
    a AS a,
    b AS b
  FROM table
  WHERE
    b = 0
)
SELECT
  a AS a,
  b AS b,
  r AS r
FROM _t0
WHERE
  r >= 3

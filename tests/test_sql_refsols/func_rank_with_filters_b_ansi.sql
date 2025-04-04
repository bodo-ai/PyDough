WITH _t1 AS (
  SELECT
    RANK() OVER (ORDER BY a) AS r,
    a AS a,
    b AS b
  FROM table
)
SELECT
  a AS a,
  b AS b,
  r AS r
FROM _t1
WHERE
  b = 0 AND r >= 3

SELECT
  SUM(a) AS a,
  b
FROM (
  SELECT
    a,
    b
  FROM table
)
GROUP BY
  b

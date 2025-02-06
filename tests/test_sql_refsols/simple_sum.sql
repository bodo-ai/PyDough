SELECT
  SUM(a) AS a
FROM (
  SELECT
    a,
    b
  FROM table
)

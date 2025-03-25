SELECT
  SUM(a) AS a,
  b
FROM (
  SELECT
    a,
    b
  FROM table
) AS _t0
GROUP BY
  b

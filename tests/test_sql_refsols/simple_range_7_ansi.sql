SELECT
  column1 + column1 AS s,
  COUNT(*) AS n,
  AVG(CAST(column1 * column1 AS DOUBLE)) AS a
FROM (VALUES
  (1),
  (2),
  (3),
  (4),
  (5),
  (6)) AS d1(x)
CROSS JOIN (VALUES
  (1),
  (2),
  (3),
  (4),
  (5),
  (6)) AS d2(y)
GROUP BY
  1

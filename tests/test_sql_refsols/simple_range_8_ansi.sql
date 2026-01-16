SELECT
  (
    column1 + column1
  ) + column1 AS s,
  COUNT(*) AS n,
  AVG(column1 * column1 * column1) AS a
FROM (VALUES
  (1),
  (2),
  (3),
  (4)) AS d1(x)
CROSS JOIN (VALUES
  (1),
  (2),
  (3),
  (4)) AS d2(y)
CROSS JOIN (VALUES
  (1),
  (2),
  (3),
  (4)) AS d3(z)
GROUP BY
  1

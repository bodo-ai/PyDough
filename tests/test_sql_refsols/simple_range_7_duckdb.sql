SELECT
  d1.x + d2.y AS s,
  COUNT(*) AS n,
  AVG(CAST(d1.x * d2.y AS DOUBLE)) AS a
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

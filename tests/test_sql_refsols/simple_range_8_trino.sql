SELECT
  (
    d1.x + d2.y
  ) + d3.z AS s,
  COUNT(*) AS n,
  AVG(CAST(d1.x * d2.y * d3.z AS DOUBLE)) AS a
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

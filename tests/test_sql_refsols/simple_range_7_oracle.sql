SELECT
  D1.X + D2.Y AS s,
  COUNT(*) AS n,
  AVG(CAST(D1.X * D2.Y AS DOUBLE PRECISION)) AS a
FROM (VALUES
  (1),
  (2),
  (3),
  (4),
  (5),
  (6)) AS D1(X)
CROSS JOIN (VALUES
  (1),
  (2),
  (3),
  (4),
  (5),
  (6)) AS D2(Y)
GROUP BY
  D1.X + D2.Y

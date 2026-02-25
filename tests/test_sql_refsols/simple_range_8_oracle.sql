SELECT
  (
    D1.X + D2.Y
  ) + D3.Z AS s,
  COUNT(*) AS n,
  AVG(D1.X * D2.Y * D3.Z) AS a
FROM (VALUES
  (1),
  (2),
  (3),
  (4)) AS D1(X)
CROSS JOIN (VALUES
  (1),
  (2),
  (3),
  (4)) AS D2(Y)
CROSS JOIN (VALUES
  (1),
  (2),
  (3),
  (4)) AS D3(Z)
GROUP BY
  (
    D1.X + D2.Y
  ) + D3.Z

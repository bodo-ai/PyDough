SELECT
  (
    COLUMN1 + COLUMN1
  ) + COLUMN1 AS s,
  COUNT(*) AS n,
  AVG(COLUMN1 * COLUMN1 * COLUMN1) AS a
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
    COLUMN1 + COLUMN1
  ) + COLUMN1

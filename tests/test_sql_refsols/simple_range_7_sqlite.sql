SELECT
  d1.column1 + d2.column1 AS s,
  COUNT(*) AS n,
  AVG(d1.column1 * d2.column1) AS a
FROM (VALUES
  (1),
  (2),
  (3),
  (4),
  (5),
  (6)) AS d1
CROSS JOIN (VALUES
  (1),
  (2),
  (3),
  (4),
  (5),
  (6)) AS d2
GROUP BY
  1

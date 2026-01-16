SELECT
  (
    d1.column1 + d2.column1
  ) + d3.column1 AS s,
  COUNT(*) AS n,
  AVG(d1.column1 * d2.column1 * d3.column1) AS a
FROM (VALUES
  (1),
  (2),
  (3),
  (4)) AS d1
CROSS JOIN (VALUES
  (1),
  (2),
  (3),
  (4)) AS d2
CROSS JOIN (VALUES
  (1),
  (2),
  (3),
  (4)) AS d3
GROUP BY
  1

SELECT
  t2.n AS N
FROM (VALUES
  (10),
  (9),
  (8),
  (7),
  (6),
  (5),
  (4),
  (3),
  (2),
  (1)) AS t2(n)
ORDER BY
  1 NULLS FIRST

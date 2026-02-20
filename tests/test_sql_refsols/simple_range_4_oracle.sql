SELECT
  COLUMN1 AS N
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
  (1)) AS T2(N)
ORDER BY
  1 NULLS FIRST

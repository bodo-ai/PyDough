SELECT
  column1 AS value
FROM (VALUES
  (0),
  (1),
  (2),
  (3),
  (4),
  (5),
  (6),
  (7),
  (8),
  (9)) AS simple_range(_col_0)
ORDER BY
  value DESC

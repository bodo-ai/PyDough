SELECT
  simple_range.column1 AS value
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
  (9)) AS simple_range
ORDER BY
  1 DESC

SELECT
  simple_range.value
FROM (VALUES
  ROW(0),
  ROW(1),
  ROW(2),
  ROW(3),
  ROW(4),
  ROW(5),
  ROW(6),
  ROW(7),
  ROW(8),
  ROW(9)) AS simple_range(value)
ORDER BY
  1 DESC

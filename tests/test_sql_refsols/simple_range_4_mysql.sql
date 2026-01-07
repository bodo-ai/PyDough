SELECT
  T2.N
FROM (VALUES
  ROW(10),
  ROW(9),
  ROW(8),
  ROW(7),
  ROW(6),
  ROW(5),
  ROW(4),
  ROW(3),
  ROW(2),
  ROW(1)) AS T2(N)
ORDER BY
  1

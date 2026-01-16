SELECT
  d1.x + d2.y AS s,
  COUNT(*) AS n,
  AVG(d1.x * d2.y) AS a
FROM (VALUES
  ROW(1),
  ROW(2),
  ROW(3),
  ROW(4),
  ROW(5),
  ROW(6)) AS d1(x)
CROSS JOIN (VALUES
  ROW(1),
  ROW(2),
  ROW(3),
  ROW(4),
  ROW(5),
  ROW(6)) AS d2(y)
GROUP BY
  1

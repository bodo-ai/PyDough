SELECT
  (
    d1.x + d2.y
  ) + d3.z AS s,
  COUNT(*) AS n,
  AVG(d1.x * d2.y * d3.z) AS a
FROM (VALUES
  ROW(1),
  ROW(2),
  ROW(3),
  ROW(4)) AS d1(x)
CROSS JOIN (VALUES
  ROW(1),
  ROW(2),
  ROW(3),
  ROW(4)) AS d2(y)
CROSS JOIN (VALUES
  ROW(1),
  ROW(2),
  ROW(3),
  ROW(4)) AS d3(z)
GROUP BY
  1

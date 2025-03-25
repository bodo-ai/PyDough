SELECT
  a * (
    b + 1
  ) AS a,
  a + (
    b * 1
  ) AS b
FROM (
  SELECT
    a,
    b
  FROM table
) AS _t0

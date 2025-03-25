SELECT
  b
FROM (
  SELECT
    a + 1 AS c,
    a,
    b
  FROM (
    SELECT
      a,
      b
    FROM table
  ) AS _t1
) AS _t0
ORDER BY
  c

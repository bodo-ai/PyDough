SELECT
  a,
  b,
  r
FROM (
  SELECT
    RANK() OVER (ORDER BY a) AS r,
    a,
    b
  FROM (
    SELECT
      a,
      b
    FROM table
  ) AS _t1
  WHERE
    b = 0
) AS _t0
WHERE
  r >= 3

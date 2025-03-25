SELECT
  a,
  b,
  r
FROM (
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
    ) AS _t2
  ) AS _t1
  WHERE
    r >= 3
) AS _t0
WHERE
  b = 0

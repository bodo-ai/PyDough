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
    )
  )
  WHERE
    r >= 3
)
WHERE
  b = 0

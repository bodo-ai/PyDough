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
  WHERE
    b = 0
)
WHERE
  r >= 3

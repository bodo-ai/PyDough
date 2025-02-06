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
  )
)
ORDER BY
  c

SELECT
  a,
  b
FROM (
  SELECT
    a,
    b
  FROM table
  ORDER BY
    a
  LIMIT 5
)
ORDER BY
  b DESC
LIMIT 2

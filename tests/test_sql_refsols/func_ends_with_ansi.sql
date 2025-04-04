SELECT
  b AS b
FROM table
WHERE
  b LIKE '%a' AND b LIKE (
    '%' || a
  )

SELECT
  b
FROM (
  SELECT
    a,
    b
  FROM table
)
WHERE
  (
    b LIKE '%a'
  ) AND (
    b LIKE (
      '%' || a
    )
  )

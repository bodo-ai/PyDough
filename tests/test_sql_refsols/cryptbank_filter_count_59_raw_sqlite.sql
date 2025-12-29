SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  'SLICE' LIKE (
    '%' || UPPER(SUBSTRING(LOWER(c_fname), 1, 1)) || '%'
  )

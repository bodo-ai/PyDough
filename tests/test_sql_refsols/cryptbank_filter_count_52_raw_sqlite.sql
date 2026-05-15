SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  LOWER(c_lname) LIKE (
    '%' || CONCAT_WS('', 'e', IIF(SUBSTRING(LOWER(c_lname), 1, 1) IN ('q', 'r', 's'), 'z', 'e')) || '%'
  )

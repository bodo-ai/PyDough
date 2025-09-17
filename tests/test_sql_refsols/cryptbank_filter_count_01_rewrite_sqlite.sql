SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  LOWER(c_lname) = 'lee'

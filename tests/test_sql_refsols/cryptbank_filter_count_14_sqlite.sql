SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_fname LIKE '%e' OR c_lname LIKE '%e'

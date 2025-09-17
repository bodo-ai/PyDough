SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  LOWER(c_fname) LIKE '%e' OR LOWER(c_lname) LIKE '%e'

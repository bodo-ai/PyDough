SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_lname <> 'lee'

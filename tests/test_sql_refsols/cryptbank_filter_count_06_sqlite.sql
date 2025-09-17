SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_email LIKE '%gmail.com'

SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_phone LIKE '555-8%'

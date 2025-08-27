SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_birthday <> '1990-07-31'

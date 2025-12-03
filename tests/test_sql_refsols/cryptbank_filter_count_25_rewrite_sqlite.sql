SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  DATE(c_birthday, '+472 days') IS NULL OR c_birthday <> '1990-07-31'

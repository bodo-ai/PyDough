SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  DATE(c_birthday, '+472 days') IS NULL
  OR c_birthday <> DATE('1991-11-15', '-472 days')

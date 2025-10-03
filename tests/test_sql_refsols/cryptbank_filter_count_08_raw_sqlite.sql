SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  DATE(c_birthday, '+472 days') = DATE('1985-04-12')

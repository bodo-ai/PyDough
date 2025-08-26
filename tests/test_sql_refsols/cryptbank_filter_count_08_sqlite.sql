SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_birthday = DATE('1985-04-12', '-472 days')

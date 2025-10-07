SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_birthday = DATE('1991-11-15', '-472 days')

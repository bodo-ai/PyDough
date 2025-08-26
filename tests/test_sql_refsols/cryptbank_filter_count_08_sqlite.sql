SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_birthday = DATE(DATETIME('1985-04-12', '-472 day'), 'start of day')

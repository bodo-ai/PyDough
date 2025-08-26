SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_birthday <> DATE(DATETIME('1991-11-15', '-472 day'), 'start of day')

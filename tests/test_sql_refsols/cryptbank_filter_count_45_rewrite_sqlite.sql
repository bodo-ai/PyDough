SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_birthday = '1989-04-07' OR c_birthday IS NULL

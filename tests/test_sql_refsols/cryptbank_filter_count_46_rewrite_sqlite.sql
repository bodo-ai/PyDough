SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_birthday IS NULL

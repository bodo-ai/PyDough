SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_birthday <> '1991-11-15' OR c_birthday IS NULL

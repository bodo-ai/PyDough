SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  NOT c_birthday IS NULL

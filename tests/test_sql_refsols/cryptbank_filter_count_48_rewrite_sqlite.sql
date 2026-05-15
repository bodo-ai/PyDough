SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  NOT c_birthday IS NULL AND c_birthday <> '1989-04-07'

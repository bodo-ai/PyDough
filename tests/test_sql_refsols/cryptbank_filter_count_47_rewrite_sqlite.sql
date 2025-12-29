SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  NOT c_birthday IN ('1990-07-31', '1989-04-07') AND NOT c_birthday IS NULL

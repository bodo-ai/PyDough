SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_birthday IN ('1990-07-31', '1989-04-07') OR c_birthday IS NULL

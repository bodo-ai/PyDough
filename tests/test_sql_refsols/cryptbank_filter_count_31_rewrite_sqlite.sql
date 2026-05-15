SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_birthday IN ('1990-07-31', '1976-10-27', '1983-12-27')

SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_birthday IN ('1980-01-18', '1981-11-15', '1990-07-31', '1994-06-15')

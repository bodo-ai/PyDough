SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  NOT c_birthday IN ('1990-07-31', '1991-03-13', '1992-05-06', '1993-01-01', '1994-06-15')

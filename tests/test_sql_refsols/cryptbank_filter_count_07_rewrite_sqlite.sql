SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_birthday IN ('1976-10-27', '1976-12-02')

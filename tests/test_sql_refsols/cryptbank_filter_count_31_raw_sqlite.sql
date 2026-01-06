SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  DATE(c_birthday, '+472 days') IN ('1991-11-15', '1978-02-11', '2005-03-14', '1985-04-12')

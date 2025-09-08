SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  SUBSTRING(c_fname, 2, 1) = 'a'

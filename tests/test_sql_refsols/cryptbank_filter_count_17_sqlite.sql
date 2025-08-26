SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  SUBSTRING(LOWER(c_fname), 2, 1) = 'a'

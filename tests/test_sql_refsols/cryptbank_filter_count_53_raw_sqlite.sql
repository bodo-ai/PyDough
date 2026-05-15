SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  SUBSTRING(LOWER(c_fname), 1, 1) = 'i'

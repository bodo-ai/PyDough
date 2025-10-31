SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  SUBSTRING(LOWER(c_fname), 2, 2) IN ('ar', 'li', 'ra', 'to', 'am')

SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  REPLACE(REPLACE(REPLACE(c_phone, '9', '*'), '0', '9'), '*', '0') = '555-123-456'

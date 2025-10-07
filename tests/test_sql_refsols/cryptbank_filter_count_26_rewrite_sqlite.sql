SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_phone = REPLACE(REPLACE(REPLACE('555-123-456', '0', '*'), '9', '0'), '*', '9')

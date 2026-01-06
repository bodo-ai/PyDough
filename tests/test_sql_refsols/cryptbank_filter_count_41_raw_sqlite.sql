SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  CONCAT_WS('-', '1', REPLACE(REPLACE(REPLACE(c_phone, '9', '*'), '0', '9'), '*', '0')) LIKE '%1-5%'

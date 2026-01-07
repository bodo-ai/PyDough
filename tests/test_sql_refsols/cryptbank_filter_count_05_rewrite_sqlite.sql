SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_phone IN ('555-809-1234', '555-870-9123')

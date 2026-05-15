SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_phone IN ('555-112-3456', '555-901-2345', '555-091-2345', '555-123-4567')

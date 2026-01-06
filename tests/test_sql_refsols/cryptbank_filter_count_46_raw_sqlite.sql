SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  COALESCE(CAST(STRFTIME('%Y', DATE(c_birthday, '+472 days')) AS INTEGER), 2005) IN (2005, 2006)

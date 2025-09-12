SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  CAST(STRFTIME('%Y', DATE(c_birthday, '+472 days')) AS INTEGER) = 1978

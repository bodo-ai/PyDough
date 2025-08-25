SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  CAST(STRFTIME('%Y', c_birthday) AS INTEGER) = 1978

SELECT
  COUNT(*) AS n
FROM crbnk.transactions
WHERE
  CAST(STRFTIME('%S', DATETIME(t_ts, '+54321 seconds')) AS INTEGER) = 23

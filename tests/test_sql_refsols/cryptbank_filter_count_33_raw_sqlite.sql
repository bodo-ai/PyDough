SELECT
  COUNT(*) AS n
FROM crbnk.accounts
WHERE
  CAST(STRFTIME('%m', DATETIME(a_open_ts, '+123456789 seconds')) AS INTEGER) IN (1, 2, 3)

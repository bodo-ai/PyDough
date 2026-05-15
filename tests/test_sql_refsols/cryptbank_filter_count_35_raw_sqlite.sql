SELECT
  COUNT(*) AS n
FROM crbnk.accounts
WHERE
  CAST(STRFTIME('%H', DATETIME(a_open_ts, '+123456789 seconds')) AS INTEGER) < 10
  AND CAST(STRFTIME('%M', DATETIME(a_open_ts, '+123456789 seconds')) AS INTEGER) < 20

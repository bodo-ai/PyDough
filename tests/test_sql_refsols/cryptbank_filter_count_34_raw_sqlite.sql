SELECT
  COUNT(*) AS n
FROM crbnk.accounts
WHERE
  CASE
    WHEN CAST(STRFTIME('%m', DATETIME(a_open_ts, '+123456789 seconds')) AS INTEGER) <= 3
    AND CAST(STRFTIME('%m', DATETIME(a_open_ts, '+123456789 seconds')) AS INTEGER) >= 1
    THEN 1
    WHEN CAST(STRFTIME('%m', DATETIME(a_open_ts, '+123456789 seconds')) AS INTEGER) <= 6
    AND CAST(STRFTIME('%m', DATETIME(a_open_ts, '+123456789 seconds')) AS INTEGER) >= 4
    THEN 2
    WHEN CAST(STRFTIME('%m', DATETIME(a_open_ts, '+123456789 seconds')) AS INTEGER) <= 9
    AND CAST(STRFTIME('%m', DATETIME(a_open_ts, '+123456789 seconds')) AS INTEGER) >= 7
    THEN 3
    WHEN CAST(STRFTIME('%m', DATETIME(a_open_ts, '+123456789 seconds')) AS INTEGER) <= 12
    AND CAST(STRFTIME('%m', DATETIME(a_open_ts, '+123456789 seconds')) AS INTEGER) >= 10
    THEN 4
  END = CAST(STRFTIME('%d', DATETIME(a_open_ts, '+123456789 seconds')) AS INTEGER)

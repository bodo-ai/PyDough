SELECT
  ROUND(AVG((
    1025.67 - t_amount
  )), 2) AS n
FROM crbnk.transactions
WHERE
  CAST(STRFTIME('%Y', DATETIME(t_ts, '+54321 seconds')) AS INTEGER) = 2022
  AND CAST(STRFTIME('%m', DATETIME(t_ts, '+54321 seconds')) AS INTEGER) = 6

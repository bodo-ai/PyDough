SELECT
  COUNT(*) AS n
FROM crbnk.transactions
WHERE
  DATETIME(t_ts, '+54321 seconds') <= '2021-05-20 12:00:00'
  AND DATETIME(t_ts, '+54321 seconds') >= '2021-05-10 12:00:00'

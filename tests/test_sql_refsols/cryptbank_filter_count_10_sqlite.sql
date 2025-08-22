SELECT
  COUNT(*) AS n
FROM crbnk.transactions
WHERE
  t_ts <= '2021-05-20 12:00:00' AND t_ts >= '2021-05-10 12:00:00'

SELECT
  COUNT(*) AS n
FROM crbnk.transactions
WHERE
  t_ts IN ('2020-11-11 09:03:02', '2023-09-15 09:00:02', '2024-07-21 23:24:02')

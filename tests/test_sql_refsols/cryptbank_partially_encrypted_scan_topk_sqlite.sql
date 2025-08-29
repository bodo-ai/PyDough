SELECT
  t_key AS key,
  t_ts AS time_stamp
FROM crbnk.transactions
ORDER BY
  2 DESC
LIMIT 5

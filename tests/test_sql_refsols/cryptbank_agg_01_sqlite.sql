SELECT
  ROUND(AVG(t_amount), 2) AS n
FROM crbnk.transactions
WHERE
  CAST(STRFTIME('%Y', t_ts) AS INTEGER) = 2022
  AND CAST(STRFTIME('%m', t_ts) AS INTEGER) = 6

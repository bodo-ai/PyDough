SELECT
  COUNT(*) AS n
FROM crbnk.transactions
WHERE
  t_amount <= 9000 AND t_amount >= 8000

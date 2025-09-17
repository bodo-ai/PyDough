SELECT
  COUNT(*) AS n
FROM crbnk.transactions
WHERE
  (
    1025.67 - t_amount
  ) <= 9000 AND (
    1025.67 - t_amount
  ) >= 8000

SELECT
  COALESCE(SUM((
    1025.67 - t_amount
  ) < 0), 0) AS n_neg,
  COALESCE(SUM((
    1025.67 - t_amount
  ) > 0), 0) AS n_positive
FROM crbnk.transactions

SELECT
  COALESCE(SUM(IIF((
    1025.67 - t_amount
  ) < 0, 1, 0)), 0) AS n_neg,
  COALESCE(SUM(IIF((
    1025.67 - t_amount
  ) > 0, 1, 0)), 0) AS n_positive
FROM crbnk.transactions

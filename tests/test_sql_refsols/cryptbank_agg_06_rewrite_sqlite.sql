SELECT
  SUM(FALSE) AS n_neg,
  SUM(TRUE) AS n_positive
FROM crbnk.transactions

WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(status = 'success') AS sum_expr_2
  FROM main.wallet_transactions_daily
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), CAST(created_at AS DATETIME), MONTH) = 1
)
SELECT
  COALESCE(sum_expr_2, 0) / n_rows AS _expr0
FROM _t0

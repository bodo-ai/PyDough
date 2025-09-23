WITH _s1 AS (
  SELECT
    receiver_id,
    COUNT(*) AS n_rows,
    SUM(amount) AS sum_amount
  FROM main.wallet_transactions_daily
  WHERE
    created_at >= DATE(DATETIME('now', '-150 day'), 'start of day')
    AND receiver_type = 1
  GROUP BY
    1
)
SELECT
  merchants.name AS merchant_name,
  _s1.n_rows AS total_transactions,
  COALESCE(_s1.sum_amount, 0) AS total_amount
FROM main.merchants AS merchants
JOIN _s1 AS _s1
  ON _s1.receiver_id = merchants.mid
ORDER BY
  3 DESC
LIMIT 2

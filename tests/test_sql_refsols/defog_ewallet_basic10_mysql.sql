WITH _s1 AS (
  SELECT
    receiver_id,
    COUNT(*) AS n_rows,
    SUM(amount) AS sum_amount
  FROM main.wallet_transactions_daily
  WHERE
    created_at >= CAST(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '150' DAY) AS DATE)
    AND receiver_type = 1
  GROUP BY
    1
)
SELECT
  merchants.name AS merchant_name,
  COALESCE(_s1.n_rows, 0) AS total_transactions,
  COALESCE(_s1.sum_amount, 0) AS total_amount
FROM main.merchants AS merchants
LEFT JOIN _s1 AS _s1
  ON _s1.receiver_id = merchants.mid
ORDER BY
  3 DESC
LIMIT 2

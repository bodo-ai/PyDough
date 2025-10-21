WITH _s1 AS (
  SELECT
    amount,
    receiver_id
  FROM main.wallet_transactions_daily
  WHERE
    created_at >= CAST(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '150' DAY) AS DATE)
    AND receiver_type = 1
), _t0 AS (
  SELECT
    _s1.receiver_id,
    ANY_VALUE(merchants.name) AS anything_name,
    COUNT(*) AS n_rows,
    SUM(_s1.amount) AS sum_amount
  FROM main.merchants AS merchants
  LEFT JOIN _s1 AS _s1
    ON _s1.receiver_id = merchants.mid
  GROUP BY
    1
)
SELECT
  anything_name AS merchant_name,
  n_rows * CASE WHEN NOT receiver_id IS NULL THEN 1 ELSE 0 END AS total_transactions,
  COALESCE(sum_amount, 0) AS total_amount
FROM _t0
ORDER BY
  3 DESC
LIMIT 2

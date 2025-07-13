WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(amount) AS sum_amount,
    receiver_id
  FROM main.wallet_transactions_daily
  WHERE
    created_at >= DATE(DATETIME('now', '-150 day'), 'start of day')
    AND receiver_type = 1
  GROUP BY
    receiver_id
), _t0 AS (
  SELECT
    merchants.name AS name_1,
    COALESCE(_s1.sum_amount, 0) AS total_amount_1,
    _s1.n_rows
  FROM main.merchants AS merchants
  LEFT JOIN _s1 AS _s1
    ON _s1.receiver_id = merchants.mid
  ORDER BY
    COALESCE(_s1.sum_amount, 0) DESC
  LIMIT 2
)
SELECT
  name_1 AS merchant_name,
  COALESCE(n_rows, 0) AS total_transactions,
  total_amount_1 AS total_amount
FROM _t0
ORDER BY
  total_amount_1 DESC

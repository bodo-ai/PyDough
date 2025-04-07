WITH _t1 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(amount) AS agg_0,
    receiver_id
  FROM main.wallet_transactions_daily
  WHERE
    created_at >= DATE(DATETIME('now', '-150 day'), 'start of day')
    AND receiver_type = 1
  GROUP BY
    receiver_id
), _t0_2 AS (
  SELECT
    merchants.name AS merchant_name,
    COALESCE(_t1.agg_0, 0) AS ordering_2,
    COALESCE(_t1.agg_0, 0) AS total_amount,
    COALESCE(_t1.agg_1, 0) AS total_transactions
  FROM main.merchants AS merchants
  LEFT JOIN _t1 AS _t1
    ON _t1.receiver_id = merchants.mid
  ORDER BY
    ordering_2 DESC
  LIMIT 2
)
SELECT
  merchant_name,
  total_transactions,
  total_amount
FROM _t0_2
ORDER BY
  ordering_2 DESC

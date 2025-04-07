WITH _t1 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(amount) AS agg_0,
    receiver_id
  FROM main.wallet_transactions_daily
  WHERE
    created_at >= DATE_TRUNC('DAY', DATE_ADD(CURRENT_TIMESTAMP(), -150, 'DAY'))
    AND receiver_type = 1
  GROUP BY
    receiver_id
)
SELECT
  merchants.name AS merchant_name,
  COALESCE(_t1.agg_1, 0) AS total_transactions,
  COALESCE(_t1.agg_0, 0) AS total_amount
FROM main.merchants AS merchants
LEFT JOIN _t1 AS _t1
  ON _t1.receiver_id = merchants.mid
ORDER BY
  total_amount DESC
LIMIT 2

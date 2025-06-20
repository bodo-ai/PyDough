WITH _s3 AS (
  SELECT
    SUM(amount) AS agg_0,
    COUNT(*) AS agg_1,
    receiver_id
  FROM main.wallet_transactions_daily
  WHERE
    created_at >= DATE_TRUNC('DAY', DATE_ADD(CURRENT_TIMESTAMP(), -150, 'DAY'))
    AND receiver_type = 1
  GROUP BY
    receiver_id
)
SELECT
  _s0.name AS merchant_name,
  COALESCE(_s3.agg_1, 0) AS total_transactions,
  COALESCE(_s3.agg_0, 0) AS total_amount
FROM main.merchants AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0.mid = _s3.receiver_id
ORDER BY
  total_amount DESC
LIMIT 2

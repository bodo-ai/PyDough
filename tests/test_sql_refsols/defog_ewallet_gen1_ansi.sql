SELECT
  MEDIAN(_s0.balance) AS _expr0
FROM main.wallet_merchant_balance_daily AS _s0
JOIN main.merchants AS _s1
  ON LOWER(_s1.category) LIKE '%retail%'
  AND _s0.merchant_id = _s1.mid
  AND _s1.status = 'active'
WHERE
  DATE_TRUNC('DAY', CAST(_s0.updated_at AS TIMESTAMP)) = DATE_TRUNC('DAY', CURRENT_TIMESTAMP())

WITH _t1 AS (
  SELECT
    wallet_merchant_balance_daily.balance AS balance,
    merchants.mid AS mid,
    wallet_merchant_balance_daily.updated_at AS updated_at
  FROM main.merchants AS merchants
  JOIN main.wallet_merchant_balance_daily AS wallet_merchant_balance_daily
    ON merchants.mid = wallet_merchant_balance_daily.merchant_id
  WHERE
    LOWER(merchants.category) LIKE '%retail%' AND merchants.status = 'active'
  QUALIFY
    DATE_TRUNC('DAY', CAST(wallet_merchant_balance_daily.updated_at AS TIMESTAMP)) = DATE_TRUNC('DAY', CURRENT_TIMESTAMP())
    AND ROW_NUMBER() OVER (PARTITION BY merchants.mid ORDER BY wallet_merchant_balance_daily.updated_at DESC NULLS FIRST) = 1
)
SELECT
  MEDIAN(_t1.balance) AS _expr0
FROM _t1 AS _t1

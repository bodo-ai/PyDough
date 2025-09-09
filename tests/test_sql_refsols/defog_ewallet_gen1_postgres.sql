WITH _t0 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY wallet_merchant_balance_daily.balance DESC NULLS LAST) - 1.0
        ) - (
          CAST((
            COUNT(wallet_merchant_balance_daily.balance) OVER () - 1.0
          ) AS DOUBLE PRECISION) / 2.0
        )
      ) < 1.0
      THEN wallet_merchant_balance_daily.balance
      ELSE NULL
    END AS expr_1
  FROM main.wallet_merchant_balance_daily AS wallet_merchant_balance_daily
  JOIN main.merchants AS merchants
    ON LOWER(merchants.category) LIKE '%retail%'
    AND merchants.mid = wallet_merchant_balance_daily.merchant_id
    AND merchants.status = 'active'
  WHERE
    DATE_TRUNC('DAY', CAST(wallet_merchant_balance_daily.updated_at AS TIMESTAMP)) = DATE_TRUNC('DAY', CURRENT_TIMESTAMP)
)
SELECT
  AVG(CAST(expr_1 AS DECIMAL)) AS _expr0
FROM _t0

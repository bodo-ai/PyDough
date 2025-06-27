WITH _t0 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY wallet_merchant_balance_daily.balance DESC) - 1.0
        ) - (
          CAST((
            COUNT(wallet_merchant_balance_daily.balance) OVER () - 1.0
          ) AS REAL) / 2.0
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
    DATE('now', 'start of day') = DATE(wallet_merchant_balance_daily.updated_at, 'start of day')
)
SELECT
  AVG(expr_1) AS _expr0
FROM _t0

WITH _t AS (
  SELECT
    wallet_merchant_balance_daily.balance,
    wallet_merchant_balance_daily.updated_at,
    ROW_NUMBER() OVER (PARTITION BY merchants.mid ORDER BY wallet_merchant_balance_daily.updated_at DESC) AS _w
  FROM main.merchants AS merchants
  JOIN main.wallet_merchant_balance_daily AS wallet_merchant_balance_daily
    ON merchants.mid = wallet_merchant_balance_daily.merchant_id
  WHERE
    LOWER(merchants.category) LIKE '%retail%' AND merchants.status = 'active'
), _t0_2 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY balance DESC) - 1.0
        ) - (
          CAST((
            COUNT(balance) OVER () - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN balance
      ELSE NULL
    END AS expr_1
  FROM _t
  WHERE
    DATE('now', 'start of day') = DATE(updated_at, 'start of day') AND _w = 1
)
SELECT
  AVG(expr_1) AS _expr0
FROM _t0_2

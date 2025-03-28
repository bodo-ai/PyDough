WITH _t AS (
  SELECT
    wallet_merchant_balance_daily.balance AS balance,
    merchants.mid AS mid,
    wallet_merchant_balance_daily.updated_at AS updated_at,
    ROW_NUMBER() OVER (PARTITION BY merchants.mid ORDER BY wallet_merchant_balance_daily.updated_at DESC) AS _w
  FROM main.merchants AS merchants
  JOIN main.wallet_merchant_balance_daily AS wallet_merchant_balance_daily
    ON merchants.mid = wallet_merchant_balance_daily.merchant_id
  WHERE
    LOWER(merchants.category) LIKE '%retail%' AND merchants.status = 'active'
), _t0 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY _t.balance DESC) - 1.0
        ) - (
          CAST((
            COUNT(_t.balance) OVER () - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN _t.balance
      ELSE NULL
    END AS expr_1
  FROM _t AS _t
  WHERE
    DATE(DATETIME('now'), 'start of day') = DATE(DATETIME(_t.updated_at), 'start of day')
    AND _t._w = 1
)
SELECT
  AVG(_t0.expr_1) AS _expr0
FROM _t0 AS _t0

WITH _t0 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY _s0.balance DESC) - 1.0
        ) - (
          CAST((
            COUNT(_s0.balance) OVER () - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN _s0.balance
      ELSE NULL
    END AS expr_1
  FROM main.wallet_merchant_balance_daily AS _s0
  JOIN main.merchants AS _s1
    ON LOWER(_s1.category) LIKE '%retail%'
    AND _s0.merchant_id = _s1.mid
    AND _s1.status = 'active'
  WHERE
    DATE('now', 'start of day') = DATE(_s0.updated_at, 'start of day')
)
SELECT
  AVG(expr_1) AS _expr0
FROM _t0

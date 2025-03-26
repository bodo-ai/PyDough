WITH _t0 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(wallet_transactions_daily.status = 'success') AS agg_0
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  WHERE
    (
      (
        CAST(STRFTIME('%Y', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%Y', wallet_transactions_daily.created_at) AS INTEGER)
      ) * 12 + CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%m', wallet_transactions_daily.created_at) AS INTEGER)
    ) = 1
)
SELECT
  CAST(COALESCE(_t0.agg_0, 0) AS REAL) / _t0.agg_1 AS _expr0
FROM _t0 AS _t0

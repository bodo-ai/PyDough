WITH _t0 AS (
  SELECT
    SUM(status = 'success') AS agg_0,
    COUNT(*) AS agg_1
  FROM main.wallet_transactions_daily
  WHERE
    (
      (
        CAST(STRFTIME('%Y', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%Y', created_at) AS INTEGER)
      ) * 12 + CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%m', created_at) AS INTEGER)
    ) = 1
)
SELECT
  CAST(COALESCE(agg_0, 0) AS REAL) / agg_1 AS _expr0
FROM _t0

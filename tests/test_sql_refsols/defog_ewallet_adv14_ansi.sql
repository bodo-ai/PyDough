WITH _t0 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(status = 'success') AS agg_0
  FROM main.wallet_transactions_daily
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), created_at, MONTH) = 1
)
SELECT
  COALESCE(agg_0, 0) / agg_1 AS _expr0
FROM _t0

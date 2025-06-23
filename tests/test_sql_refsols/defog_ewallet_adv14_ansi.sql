WITH _t0 AS (
  SELECT
    SUM(status = 'success') AS agg_0,
    COUNT(*) AS agg_1
  FROM main.wallet_transactions_daily
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), CAST(created_at AS DATETIME), MONTH) = 1
)
SELECT
  COALESCE(agg_0, 0) / agg_1 AS _expr0
FROM _t0

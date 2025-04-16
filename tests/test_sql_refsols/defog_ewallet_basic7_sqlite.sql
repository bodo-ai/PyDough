WITH _t1 AS (
  SELECT
    COUNT() AS agg_0,
    status
  FROM main.wallet_transactions_daily
  GROUP BY
    status
)
SELECT
  status,
  COALESCE(agg_0, 0) AS count
FROM _t1
ORDER BY
  count DESC
LIMIT 3

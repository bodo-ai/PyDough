WITH _t0 AS (
  SELECT
    COUNT(*) AS count,
    status
  FROM main.wallet_transactions_daily
  GROUP BY
    status
)
SELECT
  status,
  count
FROM _t0
ORDER BY
  count DESC
LIMIT 3

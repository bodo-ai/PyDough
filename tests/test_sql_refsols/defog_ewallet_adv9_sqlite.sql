WITH _t0 AS (
  SELECT
    COUNT(DISTINCT sender_id) AS agg_0,
    DATE(created_at, 'start of month') AS year_month
  FROM main.wallet_transactions_daily
  WHERE
    created_at < DATE('now', 'start of month')
    AND created_at >= DATE('now', 'start of month', '-2 month')
    AND sender_type = 0
  GROUP BY
    DATE(created_at, 'start of month')
)
SELECT
  year_month,
  COALESCE(agg_0, 0) AS active_users
FROM _t0

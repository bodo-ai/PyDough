WITH _t0 AS (
  SELECT
    COUNT(DISTINCT sender_id) AS agg_0,
    DATE_TRUNC('MONTH', CAST(created_at AS TIMESTAMP)) AS year_month
  FROM main.wallet_transactions_daily
  WHERE
    created_at < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND created_at >= DATE_ADD(DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()), -2, 'MONTH')
    AND sender_type = 0
  GROUP BY
    DATE_TRUNC('MONTH', CAST(created_at AS TIMESTAMP))
)
SELECT
  year_month,
  COALESCE(agg_0, 0) AS active_users
FROM _t0

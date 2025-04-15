WITH _t0_2 AS (
  SELECT
    COUNT(DISTINCT wallet_transactions_daily.sender_id) AS agg_0,
    DATE_TRUNC('MONTH', CAST(wallet_transactions_daily.created_at AS TIMESTAMP)) AS month
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  JOIN main.users AS users
    ON users.status = 'active' AND users.uid = wallet_transactions_daily.sender_id
  WHERE
    EXTRACT(YEAR FROM wallet_transactions_daily.created_at) = 2023
    AND wallet_transactions_daily.sender_type = 0
    AND wallet_transactions_daily.status = 'success'
  GROUP BY
    DATE_TRUNC('MONTH', CAST(wallet_transactions_daily.created_at AS TIMESTAMP))
)
SELECT
  month,
  COALESCE(agg_0, 0) AS active_users
FROM _t0_2

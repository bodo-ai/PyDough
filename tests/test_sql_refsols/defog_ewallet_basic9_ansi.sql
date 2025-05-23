WITH _t1 AS (
  SELECT
    SUM(wallet_transactions_daily.amount) AS agg_0,
    COUNT(DISTINCT wallet_transactions_daily.sender_id) AS agg_1,
    users.country
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  LEFT JOIN main.users AS users
    ON users.uid = wallet_transactions_daily.sender_id
  WHERE
    wallet_transactions_daily.sender_type = 0
  GROUP BY
    users.country
)
SELECT
  country,
  COALESCE(agg_1, 0) AS user_count,
  COALESCE(agg_0, 0) AS total_amount
FROM _t1
ORDER BY
  total_amount DESC
LIMIT 5

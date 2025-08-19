WITH _s1 AS (
  SELECT
    country,
    uid
  FROM main.users
)
SELECT
  _s1.country,
  COUNT(DISTINCT wallet_transactions_daily.sender_id) AS user_count,
  COALESCE(SUM(wallet_transactions_daily.amount), 0) AS total_amount
FROM main.wallet_transactions_daily AS wallet_transactions_daily
LEFT JOIN _s1 AS _s1
  ON _s1.uid = wallet_transactions_daily.sender_id
WHERE
  wallet_transactions_daily.sender_type = 0
GROUP BY
  1
ORDER BY
  COALESCE(SUM(wallet_transactions_daily.amount), 0) DESC
LIMIT 5

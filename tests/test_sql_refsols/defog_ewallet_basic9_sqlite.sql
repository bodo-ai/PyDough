WITH _t2 AS (
  SELECT
    COUNT(DISTINCT wallet_transactions_daily.sender_id) AS agg_1,
    SUM(wallet_transactions_daily.amount) AS agg_0,
    users.country
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  JOIN main.users AS users
    ON users.uid = wallet_transactions_daily.sender_id
  WHERE
    wallet_transactions_daily.sender_type = 0
  GROUP BY
    users.country
), _t0_2 AS (
  SELECT
    country,
    COALESCE(agg_0, 0) AS ordering_2,
    COALESCE(agg_0, 0) AS total_amount,
    agg_1 AS user_count
  FROM _t2
  ORDER BY
    ordering_2 DESC
  LIMIT 5
)
SELECT
  country,
  user_count,
  total_amount
FROM _t0_2
ORDER BY
  ordering_2 DESC

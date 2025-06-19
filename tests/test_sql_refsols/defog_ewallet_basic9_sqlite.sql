WITH _s1 AS (
  SELECT
    country,
    uid
  FROM main.users
), _t1 AS (
  SELECT
    SUM(wallet_transactions_daily.amount) AS agg_0,
    COUNT(DISTINCT wallet_transactions_daily.sender_id) AS agg_1,
    _s1.country
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  LEFT JOIN _s1 AS _s1
    ON _s1.uid = wallet_transactions_daily.sender_id
  WHERE
    wallet_transactions_daily.sender_type = 0
  GROUP BY
    _s1.country
)
SELECT
  country,
  agg_1 AS user_count,
  COALESCE(agg_0, 0) AS total_amount
FROM _t1
ORDER BY
  total_amount DESC
LIMIT 5

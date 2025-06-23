WITH _s1 AS (
  SELECT
    country,
    uid
  FROM main.users
), _t1 AS (
  SELECT
    COUNT(DISTINCT wallet_transactions_daily.sender_id) AS ndistinct_sender_id,
    SUM(wallet_transactions_daily.amount) AS sum_amount,
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
  ndistinct_sender_id AS user_count,
  COALESCE(sum_amount, 0) AS total_amount
FROM _t1
ORDER BY
  total_amount DESC
LIMIT 5

WITH _s1 AS (
  SELECT
    country,
    uid
  FROM main.users
), _t0 AS (
  SELECT
    COALESCE(SUM(wallet_transactions_daily.amount), 0) AS total_amount,
    _s1.country,
    COUNT(DISTINCT wallet_transactions_daily.sender_id) AS ndistinct_sender_id
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
  total_amount
FROM _t0
ORDER BY
  total_amount DESC
LIMIT 5

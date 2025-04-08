WITH _t0 AS (
  SELECT
    COUNT() AS agg_0,
    sender_id
  FROM main.wallet_transactions_daily
  WHERE
    sender_type = 0
  GROUP BY
    sender_id
)
SELECT
  users.uid AS user_id,
  COALESCE(_t0.agg_0, 0) AS total_transactions
FROM main.users AS users
JOIN _t0 AS _t0
  ON _t0.sender_id = users.uid

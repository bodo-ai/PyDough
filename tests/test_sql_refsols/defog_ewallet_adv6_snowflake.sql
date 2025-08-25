WITH _t0 AS (
  SELECT
    balance,
    user_id
  FROM main.wallet_user_balance_daily
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) = 1
)
SELECT
  users.uid AS user_id,
  _t0.balance AS latest_balance
FROM main.users AS users
JOIN _t0 AS _t0
  ON _t0.user_id = users.uid

WITH _t AS (
  SELECT
    balance,
    user_id,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY CASE WHEN updated_at IS NULL THEN 1 ELSE 0 END DESC, updated_at DESC) AS _w
  FROM main.wallet_user_balance_daily
)
SELECT
  users.uid AS user_id,
  _t.balance AS latest_balance
FROM main.users AS users
JOIN _t AS _t
  ON _t._w = 1 AND _t.user_id = users.uid

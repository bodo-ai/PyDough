WITH _t0 AS (
  SELECT
    balance,
    user_id
  FROM main.wallet_user_balance_daily
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC NULLS FIRST) = 1
)
SELECT
  _s0.uid AS user_id,
  _t0.balance AS latest_balance
FROM main.users AS _s0
JOIN _t0 AS _t0
  ON _s0.uid = _t0.user_id

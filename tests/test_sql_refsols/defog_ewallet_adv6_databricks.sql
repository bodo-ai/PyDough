WITH _t0 AS (
  SELECT
    balance,
    user_id
  FROM main.wallet_user_balance_daily
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC NULLS FIRST) = 1
)
SELECT
  user_id,
  balance AS latest_balance
FROM _t0

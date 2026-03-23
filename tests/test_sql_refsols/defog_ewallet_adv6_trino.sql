WITH _t AS (
  SELECT
    balance,
    user_id,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC NULLS FIRST) AS _w
  FROM postgres.main.wallet_user_balance_daily
)
SELECT
  user_id,
  balance AS latest_balance
FROM _t
WHERE
  _w = 1

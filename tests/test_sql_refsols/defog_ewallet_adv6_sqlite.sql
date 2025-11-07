WITH _t AS (
  SELECT
    balance,
    user_id,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) AS _w
  FROM main.wallet_user_balance_daily
)
SELECT
  user_id,
  balance AS latest_balance
FROM _t
WHERE
  _w = 1

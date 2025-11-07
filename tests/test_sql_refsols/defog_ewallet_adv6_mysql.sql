WITH _t AS (
  SELECT
    balance,
    user_id,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY CASE WHEN updated_at IS NULL THEN 1 ELSE 0 END DESC, updated_at DESC) AS _w
  FROM main.wallet_user_balance_daily
)
SELECT
  user_id,
  balance AS latest_balance
FROM _t
WHERE
  _w = 1

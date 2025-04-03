SELECT
  uid AS user_id,
  balance AS latest_balance
FROM (
  SELECT
    uid
  FROM main.users
)
INNER JOIN (
  SELECT
    balance,
    user_id
  FROM (
    SELECT
      *
    FROM (
      SELECT
        balance,
        updated_at,
        user_id
      FROM main.wallet_user_balance_daily
    )
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC NULLS FIRST) = 1
  )
)
  ON uid = user_id

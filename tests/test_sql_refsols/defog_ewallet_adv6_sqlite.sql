SELECT
  uid AS user_id,
  balance AS latest_balance
FROM (
  SELECT
    uid
  FROM (
    SELECT
      uid
    FROM main.users
  )
  WHERE
    EXISTS(
      SELECT
        1
      FROM (
        SELECT
          user_id
        FROM main.wallet_user_balance_daily
      )
      WHERE
        uid = user_id
    )
)
LEFT JOIN (
  SELECT
    balance,
    user_id
  FROM (
    SELECT
      *
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) AS _w
      FROM (
        SELECT
          balance,
          updated_at,
          user_id
        FROM main.wallet_user_balance_daily
      )
    ) AS _t
    WHERE
      _w = 1
  )
)
  ON uid = user_id

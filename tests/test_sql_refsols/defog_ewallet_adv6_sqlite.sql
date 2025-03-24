SELECT
  _table_alias_0.uid AS user_id,
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
) AS _table_alias_0
LEFT JOIN (
  SELECT
    balance,
    uid
  FROM (
    SELECT
      *
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY uid ORDER BY updated_at DESC) AS _w
      FROM (
        SELECT
          balance,
          uid,
          updated_at
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
        INNER JOIN (
          SELECT
            balance,
            updated_at,
            user_id
          FROM main.wallet_user_balance_daily
        )
          ON uid = user_id
      )
    ) AS _t
    WHERE
      _w = 1
  )
) AS _table_alias_1
  ON _table_alias_0.uid = _table_alias_1.uid

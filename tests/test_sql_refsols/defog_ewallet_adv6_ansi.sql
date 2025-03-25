SELECT
  _table_alias_6.uid AS user_id,
  balance AS latest_balance
FROM (
  SELECT
    uid
  FROM (
    SELECT
      uid
    FROM main.users
  ) AS _table_alias_0
  SEMI JOIN (
    SELECT
      user_id
    FROM main.wallet_user_balance_daily
  ) AS _table_alias_1
    ON uid = user_id
) AS _table_alias_6
LEFT JOIN (
  SELECT
    balance,
    uid
  FROM (
    SELECT
      *
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
        ) AS _table_alias_2
        SEMI JOIN (
          SELECT
            user_id
          FROM main.wallet_user_balance_daily
        ) AS _table_alias_3
          ON uid = user_id
      ) AS _table_alias_4
      INNER JOIN (
        SELECT
          balance,
          updated_at,
          user_id
        FROM main.wallet_user_balance_daily
      ) AS _table_alias_5
        ON uid = user_id
    ) AS _t1
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY uid ORDER BY updated_at DESC NULLS FIRST) = 1
  ) AS _t0
) AS _table_alias_7
  ON _table_alias_6.uid = _table_alias_7.uid

SELECT
  username,
  email,
  created_at
FROM (
  SELECT
    created_at,
    email,
    uid,
    username
  FROM main.users
)
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM (
      SELECT
        user_id
      FROM (
        SELECT
          _table_alias_0.created_at AS created_at,
          _table_alias_1.created_at AS created_at_1,
          user_id
        FROM (
          SELECT
            created_at,
            user_id
          FROM main.notifications
        ) AS _table_alias_0
        LEFT JOIN (
          SELECT
            created_at,
            uid
          FROM main.users
        ) AS _table_alias_1
          ON user_id = uid
      )
      WHERE
        (
          DATETIME(created_at_1, '1 year') >= created_at
        )
        AND (
          created_at >= created_at_1
        )
    )
    WHERE
      uid = user_id
  )

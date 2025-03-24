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
ANTI JOIN (
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
      DATE_ADD(CAST(created_at_1 AS TIMESTAMP), 1, 'YEAR') >= created_at
    )
    AND (
      created_at >= created_at_1
    )
)
  ON uid = user_id

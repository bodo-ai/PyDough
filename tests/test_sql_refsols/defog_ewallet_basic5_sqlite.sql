SELECT
  uid,
  username
FROM (
  SELECT
    uid,
    username
  FROM main.users
) AS _table_alias_0
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM (
      SELECT
        user_id
      FROM main.notifications
    ) AS _table_alias_1
    WHERE
      uid = user_id
  )

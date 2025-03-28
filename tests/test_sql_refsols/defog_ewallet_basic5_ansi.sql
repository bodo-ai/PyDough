WITH _table_alias_1 AS (
  SELECT
    notifications.user_id AS user_id
  FROM main.notifications AS notifications
), _table_alias_0 AS (
  SELECT
    users.uid AS uid,
    users.username AS username
  FROM main.users AS users
)
SELECT
  _table_alias_0.uid AS uid,
  _table_alias_0.username AS username
FROM _table_alias_0 AS _table_alias_0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _table_alias_1 AS _table_alias_1
    WHERE
      _table_alias_0.uid = _table_alias_1.user_id
  )

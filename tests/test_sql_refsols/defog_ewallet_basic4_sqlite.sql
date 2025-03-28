WITH _t0 AS (
  SELECT
    notifications.type AS notification_type,
    notifications.user_id AS user_id
  FROM main.notifications AS notifications
  WHERE
    notifications.type = 'transaction'
), _table_alias_1 AS (
  SELECT
    _t0.user_id AS user_id
  FROM _t0 AS _t0
), _table_alias_0 AS (
  SELECT
    users.uid AS uid
  FROM main.users AS users
)
SELECT
  _table_alias_0.uid AS user_id
FROM _table_alias_0 AS _table_alias_0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _table_alias_1 AS _table_alias_1
    WHERE
      _table_alias_0.uid = _table_alias_1.user_id
  )

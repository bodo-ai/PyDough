WITH _table_alias_0 AS (
  SELECT
    users.uid AS uid
  FROM main.users AS users
), _t0 AS (
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
), _u_0 AS (
  SELECT
    _table_alias_1.user_id AS _u_1
  FROM _table_alias_1 AS _table_alias_1
  GROUP BY
    _table_alias_1.user_id
)
SELECT
  _table_alias_0.uid AS user_id
FROM _table_alias_0 AS _table_alias_0
LEFT JOIN _u_0 AS _u_0
  ON _table_alias_0.uid = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL

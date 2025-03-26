WITH _table_alias_0 AS (
  SELECT
    users.uid AS uid,
    users.username AS username
  FROM main.users AS users
), _table_alias_1 AS (
  SELECT
    notifications.user_id AS user_id
  FROM main.notifications AS notifications
), _u_0 AS (
  SELECT
    _table_alias_1.user_id AS _u_1
  FROM _table_alias_1 AS _table_alias_1
  GROUP BY
    _table_alias_1.user_id
)
SELECT
  _table_alias_0.uid AS uid,
  _table_alias_0.username AS username
FROM _table_alias_0 AS _table_alias_0
LEFT JOIN _u_0 AS _u_0
  ON _table_alias_0.uid = _u_0._u_1
WHERE
  _u_0._u_1 IS NULL

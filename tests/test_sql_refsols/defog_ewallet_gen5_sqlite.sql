WITH _table_alias_2 AS (
  SELECT
    users.created_at AS created_at,
    users.email AS email,
    users.uid AS uid,
    users.username AS username
  FROM main.users AS users
), _table_alias_0 AS (
  SELECT
    notifications.created_at AS created_at,
    notifications.user_id AS user_id
  FROM main.notifications AS notifications
), _table_alias_1 AS (
  SELECT
    users.created_at AS created_at,
    users.uid AS uid
  FROM main.users AS users
), _t0 AS (
  SELECT
    _table_alias_0.created_at AS created_at,
    _table_alias_1.created_at AS created_at_1,
    _table_alias_0.user_id AS user_id
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.user_id = _table_alias_1.uid
  WHERE
    _table_alias_0.created_at <= DATETIME(_table_alias_1.created_at, '1 year')
    AND _table_alias_0.created_at >= _table_alias_1.created_at
), _table_alias_3 AS (
  SELECT
    _t0.user_id AS user_id
  FROM _t0 AS _t0
), _u_0 AS (
  SELECT
    _table_alias_3.user_id AS _u_1
  FROM _table_alias_3 AS _table_alias_3
  GROUP BY
    _table_alias_3.user_id
)
SELECT
  _table_alias_2.username AS username,
  _table_alias_2.email AS email,
  _table_alias_2.created_at AS created_at
FROM _table_alias_2 AS _table_alias_2
LEFT JOIN _u_0 AS _u_0
  ON _table_alias_2.uid = _u_0._u_1
WHERE
  _u_0._u_1 IS NULL

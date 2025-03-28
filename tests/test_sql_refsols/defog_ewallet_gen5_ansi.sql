WITH _table_alias_0 AS (
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
    _table_alias_0.created_at <= DATE_ADD(CAST(_table_alias_1.created_at AS TIMESTAMP), 1, 'YEAR')
    AND _table_alias_0.created_at >= _table_alias_1.created_at
), _table_alias_3 AS (
  SELECT
    _t0.user_id AS user_id
  FROM _t0 AS _t0
), _table_alias_2 AS (
  SELECT
    users.created_at AS created_at,
    users.email AS email,
    users.uid AS uid,
    users.username AS username
  FROM main.users AS users
)
SELECT
  _table_alias_2.username AS username,
  _table_alias_2.email AS email,
  _table_alias_2.created_at AS created_at
FROM _table_alias_2 AS _table_alias_2
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _table_alias_3 AS _table_alias_3
    WHERE
      _table_alias_2.uid = _table_alias_3.user_id
  )

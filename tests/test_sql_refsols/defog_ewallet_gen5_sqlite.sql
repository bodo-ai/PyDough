WITH _s0 AS (
  SELECT
    notifications.created_at AS created_at,
    notifications.user_id AS user_id
  FROM main.notifications AS notifications
), _s1 AS (
  SELECT
    users.created_at AS created_at,
    users.uid AS uid
  FROM main.users AS users
), _t0 AS (
  SELECT
    _s0.created_at AS created_at,
    _s1.created_at AS created_at_1,
    _s0.user_id AS user_id
  FROM _s0 AS _s0
  LEFT JOIN _s1 AS _s1
    ON _s0.user_id = _s1.uid
  WHERE
    _s0.created_at <= DATETIME(_s1.created_at, '1 year')
    AND _s0.created_at >= _s1.created_at
), _s3 AS (
  SELECT
    _t0.user_id AS user_id
  FROM _t0 AS _t0
), _s2 AS (
  SELECT
    users.created_at AS created_at,
    users.email AS email,
    users.uid AS uid,
    users.username AS username
  FROM main.users AS users
)
SELECT
  _s2.username AS username,
  _s2.email AS email,
  _s2.created_at AS created_at
FROM _s2 AS _s2
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _s3 AS _s3
    WHERE
      _s2.uid = _s3.user_id
  )

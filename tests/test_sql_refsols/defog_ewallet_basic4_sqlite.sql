WITH _t0 AS (
  SELECT
    notifications.type AS notification_type,
    notifications.user_id AS user_id
  FROM main.notifications AS notifications
), _s1 AS (
  SELECT
    _t0.user_id AS user_id
  FROM _t0 AS _t0
  WHERE
    _t0.notification_type = 'transaction'
), _s0 AS (
  SELECT
    users.uid AS user_id
  FROM main.users AS users
)
SELECT
  _s0.user_id AS user_id
FROM _s0 AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0.user_id = _s1.user_id
  )

WITH _t0 AS (
  SELECT
    notifications.type AS notification_type,
    notifications.user_id AS user_id
  FROM main.notifications AS notifications
  WHERE
    notifications.type = 'transaction'
), _t1 AS (
  SELECT
    _t0.user_id AS user_id
  FROM _t0 AS _t0
), _t0_2 AS (
  SELECT
    users.uid AS uid,
    users.uid AS user_id
  FROM main.users AS users
)
SELECT
  _t0.user_id AS user_id
FROM _t0_2 AS _t0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _t1 AS _t1
    WHERE
      _t0.uid = _t1.user_id
  )

WITH _t1 AS (
  SELECT
    notifications.user_id AS user_id
  FROM main.notifications AS notifications
), _t0 AS (
  SELECT
    users.uid AS uid,
    users.username AS username
  FROM main.users AS users
)
SELECT
  _t0.uid AS uid,
  _t0.username AS username
FROM _t0 AS _t0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _t1 AS _t1
    WHERE
      _t0.uid = _t1.user_id
  )

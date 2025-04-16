WITH _s1 AS (
  SELECT
    notifications.user_id AS user_id
  FROM main.notifications AS notifications
), _s0 AS (
  SELECT
    users.uid AS uid,
    users.username AS username
  FROM main.users AS users
)
SELECT
  _s0.uid AS uid,
  _s0.username AS username
FROM _s0 AS _s0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0.uid = _s1.user_id
  )

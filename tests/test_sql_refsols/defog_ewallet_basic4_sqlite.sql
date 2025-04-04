WITH _t0 AS (
  SELECT
    type AS notification_type,
    user_id AS user_id
  FROM main.notifications
  WHERE
    type = 'transaction'
), _t1 AS (
  SELECT
    user_id AS user_id
  FROM _t0
), _t0_2 AS (
  SELECT
    uid AS uid
  FROM main.users
)
SELECT
  _t0.uid AS user_id
FROM _t0_2 AS _t0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _t1
    WHERE
      uid = user_id
  )

WITH _t1 AS (
  SELECT
    user_id AS user_id
  FROM main.notifications
), _t0 AS (
  SELECT
    uid AS uid,
    username AS username
  FROM main.users
)
SELECT
  uid AS uid,
  username AS username
FROM _t0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _t1
    WHERE
      uid = user_id
  )

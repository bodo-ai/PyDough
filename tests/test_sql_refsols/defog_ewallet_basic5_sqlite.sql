WITH _u_0 AS (
  SELECT
    notifications.user_id AS _u_1
  FROM main.notifications AS notifications
  GROUP BY
    notifications.user_id
)
SELECT
  users.uid AS uid,
  users.username AS username
FROM main.users AS users
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = users.uid
WHERE
  _u_0._u_1 IS NULL

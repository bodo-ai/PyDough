WITH _u_0 AS (
  SELECT
    notifications.user_id AS _u_1
  FROM main.notifications AS notifications
  JOIN main.users AS users
    ON notifications.user_id = users.uid
  WHERE
    notifications.created_at <= DATETIME(users.created_at, '1 year')
    AND notifications.created_at >= users.created_at
  GROUP BY
    notifications.user_id
)
SELECT
  users.username AS username,
  users.email AS email,
  users.created_at AS created_at
FROM main.users AS users
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = users.uid
WHERE
  _u_0._u_1 IS NULL

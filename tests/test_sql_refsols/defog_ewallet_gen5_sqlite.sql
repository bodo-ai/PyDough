SELECT
  username,
  email,
  created_at
FROM main.users
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.notifications AS notifications
    JOIN main.users AS users
      ON notifications.created_at <= DATETIME(users.created_at, '1 year')
      AND notifications.created_at >= users.created_at
      AND notifications.user_id = users.uid
    WHERE
      notifications.user_id = users.uid
  )

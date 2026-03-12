SELECT
  username,
  email,
  created_at
FROM ewallet.users
WHERE
  NOT EXISTS(
    SELECT
      1 AS `1`
    FROM ewallet.notifications AS notifications
    JOIN ewallet.users AS users
      ON notifications.created_at <= DATE_ADD(CAST(users.created_at AS DATETIME), INTERVAL '1' YEAR)
      AND notifications.created_at >= users.created_at
      AND notifications.user_id = users.uid
    WHERE
      notifications.user_id = users.uid
  )

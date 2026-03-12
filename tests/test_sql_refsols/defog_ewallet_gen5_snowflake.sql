SELECT
  username,
  email,
  created_at
FROM ewallet.users
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM ewallet.notifications AS notifications
    JOIN ewallet.users AS users
      ON notifications.created_at <= DATEADD(YEAR, 1, CAST(users.created_at AS TIMESTAMP))
      AND notifications.created_at >= users.created_at
      AND notifications.user_id = users.uid
    WHERE
      notifications.user_id = users.uid
  )

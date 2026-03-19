SELECT
  users.username,
  users.email,
  users.created_at
FROM main.users AS users
JOIN main.notifications AS notifications
  ON notifications.user_id = users.uid
JOIN main.users AS users_2
  ON notifications.created_at <= DATE_ADD(CAST(users_2.created_at AS TIMESTAMP), 1, 'YEAR')
  AND notifications.created_at >= users_2.created_at
  AND notifications.user_id = users_2.uid

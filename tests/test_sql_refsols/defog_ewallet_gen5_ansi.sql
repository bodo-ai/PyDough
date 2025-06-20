SELECT
  users.username AS username,
  users.email AS email,
  users.created_at AS created_at
FROM main.users AS users
JOIN main.notifications AS notifications
  ON notifications.user_id = users.uid
JOIN main.users AS users_2
  ON notifications.user_id = users_2.uid
WHERE
  notifications.created_at <= DATE_ADD(CAST(users_2.created_at AS TIMESTAMP), 1, 'YEAR')
  AND notifications.created_at >= users_2.created_at

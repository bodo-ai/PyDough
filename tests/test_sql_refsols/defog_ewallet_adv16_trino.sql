SELECT
  ARBITRARY(users.username) AS username,
  COUNT(*) AS total_unread_notifs
FROM postgres.main.users AS users
JOIN postgres.main.notifications AS notifications
  ON notifications.status = 'unread'
  AND notifications.type = 'promotion'
  AND notifications.user_id = users.uid
WHERE
  LOWER(users.country) = 'us'
GROUP BY
  notifications.user_id

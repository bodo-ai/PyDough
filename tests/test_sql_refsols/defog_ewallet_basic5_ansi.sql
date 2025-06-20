SELECT
  users.uid AS uid,
  users.username AS username
FROM main.users AS users
JOIN main.notifications AS notifications
  ON notifications.user_id = users.uid

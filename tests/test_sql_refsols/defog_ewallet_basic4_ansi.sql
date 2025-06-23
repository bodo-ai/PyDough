SELECT
  users.uid AS user_id
FROM main.users AS users
JOIN main.notifications AS notifications
  ON notifications.type = 'transaction' AND notifications.user_id = users.uid

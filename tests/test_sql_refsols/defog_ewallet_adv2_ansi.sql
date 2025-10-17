SELECT
  DATE_TRUNC('WEEK', CAST(notifications.created_at AS TIMESTAMP)) AS week,
  COUNT(*) AS num_notifs,
  COALESCE(SUM((
    (
      DAY_OF_WEEK(notifications.created_at) + 6
    ) % 7
  ) IN (5, 6)), 0) AS weekend_notifs
FROM main.notifications AS notifications
JOIN main.users AS users
  ON notifications.user_id = users.uid AND users.country IN ('US', 'CA')
WHERE
  notifications.created_at < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
  AND notifications.created_at >= DATE_SUB(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), 3, WEEK)
GROUP BY
  1

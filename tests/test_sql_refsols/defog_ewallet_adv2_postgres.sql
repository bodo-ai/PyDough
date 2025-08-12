SELECT
  DATE_TRUNC('WEEK', CAST(notifications.created_at AS TIMESTAMP)) AS week,
  COUNT(*) AS num_notifs,
  COALESCE(
    SUM(
      CASE
        WHEN (
          (
            EXTRACT(DOW FROM CAST(notifications.created_at AS TIMESTAMP)) + 6
          ) % 7
        ) IN (5, 6)
        THEN 1
        ELSE 0
      END
    ),
    0
  ) AS weekend_notifs
FROM main.notifications AS notifications
JOIN main.users AS users
  ON notifications.user_id = users.uid AND users.country IN ('US', 'CA')
WHERE
  notifications.created_at < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP)
  AND notifications.created_at >= DATE_TRUNC('WEEK', CURRENT_TIMESTAMP) + INTERVAL '3 WEEK'
GROUP BY
  DATE_TRUNC('WEEK', CAST(notifications.created_at AS TIMESTAMP))

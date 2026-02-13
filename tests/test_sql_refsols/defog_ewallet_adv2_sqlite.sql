SELECT
  DATE(
    notifications.created_at,
    '-' || CAST((
      CAST(STRFTIME('%w', DATETIME(notifications.created_at)) AS INTEGER) + 6
    ) % 7 AS TEXT) || ' days',
    'start of day'
  ) AS week,
  COUNT(*) AS num_notifs,
  COALESCE(
    SUM(
      (
        (
          CAST(STRFTIME('%w', notifications.created_at) AS INTEGER) + 6
        ) % 7
      ) IN (5, 6)
    ),
    0
  ) AS weekend_notifs
FROM main.notifications AS notifications
JOIN main.users AS users
  ON notifications.user_id = users.uid AND users.country IN ('US', 'CA')
WHERE
  notifications.created_at < DATE(
    'now',
    '-' || CAST((
      CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
    ) % 7 AS TEXT) || ' days',
    'start of day'
  )
  AND notifications.created_at >= DATE(
    'now',
    '-' || CAST((
      CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
    ) % 7 AS TEXT) || ' days',
    'start of day',
    '-21 day'
  )
GROUP BY
  1

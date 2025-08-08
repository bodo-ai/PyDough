SELECT
  DATE(
    DATE_SUB(
      CAST(notifications.created_at AS DATETIME),
      INTERVAL (
        (
          DAYOFWEEK(CAST(notifications.created_at AS DATETIME)) + 5
        ) % 7
      ) DAY
    )
  ) AS week,
  COUNT(*) AS num_notifs,
  COALESCE(SUM((
    (
      DAYOFWEEK(notifications.created_at) + 5
    ) % 7
  ) IN (5, 6)), 0) AS weekend_notifs
FROM main.notifications AS notifications
JOIN main.users AS users
  ON notifications.user_id = users.uid AND users.country IN ('US', 'CA')
WHERE
  notifications.created_at < DATE(
    DATE_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      ) DAY
    )
  )
  AND notifications.created_at >= DATE_ADD(
    DATE_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      ) DAY
    ),
    INTERVAL '-3' WEEK
  )
GROUP BY
  DATE(
    DATE_SUB(
      CAST(notifications.created_at AS DATETIME),
      INTERVAL (
        (
          DAYOFWEEK(CAST(notifications.created_at AS DATETIME)) + 5
        ) % 7
      ) DAY
    )
  )

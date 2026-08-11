SELECT
  CAST(CAST(notifications.created_at AS TIMESTAMP) AS DATE) - CAST((
    (
      DAYOFWEEK(CAST(notifications.created_at AS TIMESTAMP)) + 6
    ) % 7
  ) AS INT) AS week,
  COUNT(*) AS num_notifs,
  COUNT_IF((
    (
      DAYOFWEEK(notifications.created_at) + 6
    ) % 7
  ) IN (5, 6)) AS weekend_notifs
FROM main.notifications AS notifications
JOIN main.users AS users
  ON notifications.user_id = users.uid AND users.country IN ('US', 'CA')
WHERE
  notifications.created_at < (
    CAST(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS DATE) - CAST((
      (
        DAYOFWEEK(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)) + 6
      ) % 7
    ) AS INT)
  )
  AND notifications.created_at >= CAST(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) AS DATE) - CAST((
    (
      DAYOFWEEK(CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)) + 6
    ) % 7
  ) AS INT) - 7 * INTERVAL '3' DAY
GROUP BY
  1

SELECT
  DATE_TRUNC(
    'DAY',
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CAST(notifications.created_at AS TIMESTAMP)) + 6
        ) % 7
      ) * -1,
      CAST(notifications.created_at AS TIMESTAMP)
    )
  ) AS week,
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
  notifications.created_at < DATE_TRUNC(
    'DAY',
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 6
        ) % 7
      ) * -1,
      CURRENT_TIMESTAMP()
    )
  )
  AND notifications.created_at >= DATEADD(
    WEEK,
    -3,
    DATE_TRUNC(
      'DAY',
      DATEADD(
        DAY,
        (
          (
            DAYOFWEEK(CURRENT_TIMESTAMP()) + 6
          ) % 7
        ) * -1,
        CURRENT_TIMESTAMP()
      )
    )
  )
GROUP BY
  1

SELECT
  DATE_TRUNC(
    'DAY',
    DATE_ADD(
      'DAY',
      (
        (
          (
            (
              DAY_OF_WEEK(CAST(notifications.created_at AS TIMESTAMP)) % 7
            ) + 1
          ) + -1
        ) % 7
      ) * -1,
      CAST(notifications.created_at AS TIMESTAMP)
    )
  ) AS week,
  COUNT(*) AS num_notifs,
  COUNT_IF(
    (
      (
        (
          DAY_OF_WEEK(notifications.created_at) % 7
        ) + 0
      ) % 7
    ) IN (5, 6)
  ) AS weekend_notifs
FROM postgres.notifications AS notifications
JOIN postgres.users AS users
  ON notifications.user_id = users.uid AND users.country IN ('US', 'CA')
WHERE
  notifications.created_at < DATE_TRUNC(
    'DAY',
    DATE_ADD(
      'DAY',
      (
        (
          (
            DAY_OF_WEEK(CURRENT_TIMESTAMP) % 7
          ) + 0
        ) % 7
      ) * -1,
      CURRENT_TIMESTAMP
    )
  )
  AND notifications.created_at >= DATE_ADD(
    'WEEK',
    -3,
    DATE_TRUNC(
      'DAY',
      DATE_ADD(
        'DAY',
        (
          (
            (
              DAY_OF_WEEK(CURRENT_TIMESTAMP) % 7
            ) + 0
          ) % 7
        ) * -1,
        CURRENT_TIMESTAMP
      )
    )
  )
GROUP BY
  1

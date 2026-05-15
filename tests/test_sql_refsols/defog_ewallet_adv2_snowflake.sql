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
FROM ewallet.notifications AS notifications
JOIN ewallet.users AS users
  ON notifications.user_id = users.uid AND users.country IN ('US', 'CA')
WHERE
  notifications.created_at < DATE_TRUNC(
    'DAY',
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)) + 6
        ) % 7
      ) * -1,
      CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)
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
            DAYOFWEEK(CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)) + 6
          ) % 7
        ) * -1,
        CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)
      )
    )
  )
GROUP BY
  1

SELECT
  DATE_TRUNC(
    'DAY',
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CAST(NOTIFICATIONS.created_at AS TIMESTAMP)) + 6
        ) % 7
      ) * -1,
      CAST(NOTIFICATIONS.created_at AS TIMESTAMP)
    )
  ) AS week,
  COUNT(*) AS num_notifs,
  COALESCE(COUNT_IF((
    (
      DAYOFWEEK(NOTIFICATIONS.created_at) + 6
    ) % 7
  ) IN (5, 6)), 0) AS weekend_notifs
FROM MAIN.NOTIFICATIONS AS NOTIFICATIONS
JOIN MAIN.USERS AS USERS
  ON NOTIFICATIONS.user_id = USERS.uid AND USERS.country IN ('US', 'CA')
WHERE
  NOTIFICATIONS.created_at < DATE_TRUNC(
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
  AND NOTIFICATIONS.created_at >= DATEADD(
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
  DATE_TRUNC(
    'DAY',
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CAST(NOTIFICATIONS.created_at AS TIMESTAMP)) + 6
        ) % 7
      ) * -1,
      CAST(NOTIFICATIONS.created_at AS TIMESTAMP)
    )
  )

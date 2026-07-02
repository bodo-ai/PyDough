SELECT
  DATEADD(
    DAY,
    -(
      (
        DAYOFWEEK(TO_DATE(CAST(notifications.created_at AS TIMESTAMP))) + 5
      ) % 7
    ),
    CAST(CAST(notifications.created_at AS TIMESTAMP) AS DATE)
  ) AS week,
  COUNT(*) AS num_notifs,
  COUNT_IF((
    (
      DAYOFWEEK(TO_DATE(notifications.created_at)) + 5
    ) % 7
  ) IN (5, 6)) AS weekend_notifs
FROM defog.ewallet.notifications AS notifications
JOIN defog.ewallet.users AS users
  ON notifications.user_id = users.uid AND users.country IN ('US', 'CA')
WHERE
  notifications.created_at < DATEADD(
    DAY,
    -(
      (
        DAYOFWEEK(TO_DATE(CURRENT_TIMESTAMP())) + 5
      ) % 7
    ),
    CAST(CURRENT_TIMESTAMP() AS DATE)
  )
  AND notifications.created_at >= DATEADD(
    DAY,
    -21,
    DATEADD(
      DAY,
      -(
        (
          DAYOFWEEK(TO_DATE(CURRENT_TIMESTAMP())) + 5
        ) % 7
      ),
      CAST(CURRENT_TIMESTAMP() AS DATE)
    )
  )
GROUP BY
  1

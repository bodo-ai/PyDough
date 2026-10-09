SELECT
  DATE_ADD(
    CAST(CAST(notifications.created_at AS TIMESTAMP) AS DATE),
    -(
      (
        DAYOFWEEK(CAST(notifications.created_at AS TIMESTAMP)) + 5
      ) % 7
    )
  ) AS week,
  COUNT(*) AS num_notifs,
  COUNT_IF((
    (
      DAYOFWEEK(notifications.created_at) + 5
    ) % 7
  ) IN (5, 6)) AS weekend_notifs
FROM defog.ewallet.notifications AS notifications
JOIN defog.ewallet.users AS users
  ON notifications.user_id = users.uid AND users.country IN ('US', 'CA')
WHERE
  notifications.created_at < DATE_ADD(
    CAST(CURRENT_TIMESTAMP() AS DATE),
    -(
      (
        DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
      ) % 7
    )
  )
  AND notifications.created_at >= DATE_ADD(
    DATE_ADD(
      CAST(CURRENT_TIMESTAMP() AS DATE),
      -(
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 5
        ) % 7
      )
    ),
    -21
  )
GROUP BY
  1

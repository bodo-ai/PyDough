SELECT
  STR_TO_DATE(
    CONCAT(
      YEAR(CAST(notifications.created_at AS DATETIME)),
      ' ',
      WEEK(CAST(notifications.created_at AS DATETIME), 1),
      ' 1'
    ),
    '%Y %u %w'
  ) AS week,
  COUNT(*) AS num_notifs,
  COALESCE(SUM((
    (
      DAYOFWEEK(notifications.created_at) + 6
    ) % 7
  ) IN (5, 6)), 0) AS weekend_notifs
FROM main.notifications AS notifications
JOIN main.users AS users
  ON notifications.user_id = users.uid AND users.country IN ('US', 'CA')
WHERE
  notifications.created_at < STR_TO_DATE(
    CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', WEEK(CURRENT_TIMESTAMP(), 1), ' 1'),
    '%Y %u %w'
  )
  AND notifications.created_at >= DATE_ADD(
    STR_TO_DATE(
      CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', WEEK(CURRENT_TIMESTAMP(), 1), ' 1'),
      '%Y %u %w'
    ),
    INTERVAL '-3' WEEK
  )
GROUP BY
  STR_TO_DATE(
    CONCAT(
      YEAR(CAST(notifications.created_at AS DATETIME)),
      ' ',
      WEEK(CAST(notifications.created_at AS DATETIME), 1),
      ' 1'
    ),
    '%Y %u %w'
  )

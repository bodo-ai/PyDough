WITH _t0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM((
      (
        DAY_OF_WEEK(notifications.created_at) + 6
      ) % 7
    ) IN (5, 6)) AS agg_1,
    DATE_TRUNC('WEEK', CAST(notifications.created_at AS TIMESTAMP)) AS week
  FROM main.notifications AS notifications
  JOIN main.users AS users
    ON notifications.user_id = users.uid AND users.country IN ('US', 'CA')
  WHERE
    notifications.created_at < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND notifications.created_at >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -3, 'WEEK')
  GROUP BY
    DATE_TRUNC('WEEK', CAST(notifications.created_at AS TIMESTAMP))
)
SELECT
  week,
  agg_0 AS num_notifs,
  COALESCE(agg_1, 0) AS weekend_notifs
FROM _t0

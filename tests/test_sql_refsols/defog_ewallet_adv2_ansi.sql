WITH _t0_2 AS (
  SELECT
    COUNT() AS agg_0,
    SUM((
      (
        DAY_OF_WEEK(notifications.created_at) + 6
      ) % 7
    ) IN (5, 6)) AS agg_1,
    DATE_TRUNC('WEEK', CAST(notifications.created_at AS TIMESTAMP)) AS week
  FROM main.users AS users
  JOIN main.notifications AS notifications
    ON notifications.created_at < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND notifications.created_at >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -3, 'WEEK')
    AND notifications.user_id = users.uid
  WHERE
    users.country IN ('US', 'CA')
  GROUP BY
    DATE_TRUNC('WEEK', CAST(notifications.created_at AS TIMESTAMP))
)
SELECT
  _t0.week AS week,
  COALESCE(_t0.agg_0, 0) AS num_notifs,
  COALESCE(_t0.agg_1, 0) AS weekend_notifs
FROM _t0_2 AS _t0

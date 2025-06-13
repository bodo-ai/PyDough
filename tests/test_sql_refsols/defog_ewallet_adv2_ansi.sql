WITH _s0 AS (
  SELECT
    COUNT() AS agg_0,
    SUM((
      (
        DAY_OF_WEEK(created_at) + 6
      ) % 7
    ) IN (5, 6)) AS agg_1,
    user_id,
    DATE_TRUNC('WEEK', CAST(created_at AS TIMESTAMP)) AS week
  FROM main.notifications
  WHERE
    created_at < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND created_at >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -3, 'WEEK')
  GROUP BY
    user_id,
    DATE_TRUNC('WEEK', CAST(created_at AS TIMESTAMP))
), _t0 AS (
  SELECT
    SUM(_s0.agg_0) AS agg_0,
    SUM(_s0.agg_1) AS agg_1,
    _s0.week
  FROM _s0 AS _s0
  JOIN main.users AS users
    ON _s0.user_id = users.uid AND users.country IN ('US', 'CA')
  GROUP BY
    _s0.week
)
SELECT
  week,
  agg_0 AS num_notifs,
  COALESCE(agg_1, 0) AS weekend_notifs
FROM _t0

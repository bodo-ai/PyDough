WITH _s1 AS (
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
    SUM(_s1.agg_0) AS agg_0,
    SUM(_s1.agg_1) AS agg_1,
    _s1.week
  FROM main.users AS users
  JOIN _s1 AS _s1
    ON _s1.user_id = users.uid
  WHERE
    users.country IN ('US', 'CA')
  GROUP BY
    _s1.week
)
SELECT
  week,
  COALESCE(agg_0, 0) AS num_notifs,
  COALESCE(agg_1, 0) AS weekend_notifs
FROM _t0

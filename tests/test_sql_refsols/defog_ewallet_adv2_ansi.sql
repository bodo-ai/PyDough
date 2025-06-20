WITH _t0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM((
      (
        DAY_OF_WEEK(_s0.created_at) + 6
      ) % 7
    ) IN (5, 6)) AS agg_1,
    DATE_TRUNC('WEEK', CAST(_s0.created_at AS TIMESTAMP)) AS week
  FROM main.notifications AS _s0
  JOIN main.users AS _s1
    ON _s0.user_id = _s1.uid AND _s1.country IN ('US', 'CA')
  WHERE
    _s0.created_at < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND _s0.created_at >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -3, 'WEEK')
  GROUP BY
    DATE_TRUNC('WEEK', CAST(_s0.created_at AS TIMESTAMP))
)
SELECT
  week,
  agg_0 AS num_notifs,
  COALESCE(agg_1, 0) AS weekend_notifs
FROM _t0

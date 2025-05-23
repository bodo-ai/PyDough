WITH _s1 AS (
  SELECT
    COUNT() AS agg_0,
    SUM((
      (
        CAST(STRFTIME('%w', created_at) AS INTEGER) + 6
      ) % 7
    ) IN (5, 6)) AS agg_1,
    user_id,
    DATE(
      created_at,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(created_at)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    ) AS week
  FROM main.notifications
  WHERE
    created_at < DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
    AND created_at >= DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day',
      '-21 day'
    )
  GROUP BY
    user_id,
    DATE(
      created_at,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(created_at)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
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
  agg_0 AS num_notifs,
  COALESCE(agg_1, 0) AS weekend_notifs
FROM _t0

WITH _t0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM(
      (
        (
          CAST(STRFTIME('%w', _s0.created_at) AS INTEGER) + 6
        ) % 7
      ) IN (5, 6)
    ) AS agg_1,
    DATE(
      _s0.created_at,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(_s0.created_at)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    ) AS week
  FROM main.notifications AS _s0
  JOIN main.users AS _s1
    ON _s0.user_id = _s1.uid AND _s1.country IN ('US', 'CA')
  WHERE
    _s0.created_at < DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
    AND _s0.created_at >= DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day',
      '-21 day'
    )
  GROUP BY
    DATE(
      _s0.created_at,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(_s0.created_at)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
)
SELECT
  week,
  agg_0 AS num_notifs,
  COALESCE(agg_1, 0) AS weekend_notifs
FROM _t0

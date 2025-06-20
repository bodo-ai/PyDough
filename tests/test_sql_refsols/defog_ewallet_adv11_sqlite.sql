WITH _t0 AS (
  SELECT
    SUM(
      (
        (
          CAST((
            JULIANDAY(DATE(session_end_ts, 'start of day')) - JULIANDAY(DATE(session_start_ts, 'start of day'))
          ) AS INTEGER) * 24 + CAST(STRFTIME('%H', session_end_ts) AS INTEGER) - CAST(STRFTIME('%H', session_start_ts) AS INTEGER)
        ) * 60 + CAST(STRFTIME('%M', session_end_ts) AS INTEGER) - CAST(STRFTIME('%M', session_start_ts) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%S', session_end_ts) AS INTEGER) - CAST(STRFTIME('%S', session_start_ts) AS INTEGER)
    ) AS agg_0,
    user_id
  FROM main.user_sessions
  WHERE
    session_end_ts < '2023-06-08' AND session_start_ts >= '2023-06-01'
  GROUP BY
    user_id
)
SELECT
  _s0.uid,
  COALESCE(_t0.agg_0, 0) AS total_duration
FROM main.users AS _s0
JOIN _t0 AS _t0
  ON _s0.uid = _t0.user_id
ORDER BY
  total_duration DESC

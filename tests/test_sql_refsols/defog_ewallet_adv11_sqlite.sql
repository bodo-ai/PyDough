WITH _s1 AS (
  SELECT
    COALESCE(
      SUM(
        (
          (
            CAST((
              JULIANDAY(DATE(session_end_ts, 'start of day')) - JULIANDAY(DATE(session_start_ts, 'start of day'))
            ) AS INTEGER) * 24 + CAST(STRFTIME('%H', session_end_ts) AS INTEGER) - CAST(STRFTIME('%H', session_start_ts) AS INTEGER)
          ) * 60 + CAST(STRFTIME('%M', session_end_ts) AS INTEGER) - CAST(STRFTIME('%M', session_start_ts) AS INTEGER)
        ) * 60 + CAST(STRFTIME('%S', session_end_ts) AS INTEGER) - CAST(STRFTIME('%S', session_start_ts) AS INTEGER)
      ),
      0
    ) AS total_duration,
    user_id
  FROM main.user_sessions
  WHERE
    session_end_ts < '2023-06-08' AND session_start_ts >= '2023-06-01'
  GROUP BY
    user_id
)
SELECT
  users.uid,
  _s1.total_duration
FROM main.users AS users
JOIN _s1 AS _s1
  ON _s1.user_id = users.uid
ORDER BY
  total_duration DESC

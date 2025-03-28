SELECT
  user_sessions.device_type AS device_type,
  AVG(
    (
      (
        CAST((
          JULIANDAY(DATE(user_sessions.session_end_ts, 'start of day')) - JULIANDAY(DATE(user_sessions.session_start_ts, 'start of day'))
        ) AS INTEGER) * 24 + CAST(STRFTIME('%H', user_sessions.session_end_ts) AS INTEGER) - CAST(STRFTIME('%H', user_sessions.session_start_ts) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%M', user_sessions.session_end_ts) AS INTEGER) - CAST(STRFTIME('%M', user_sessions.session_start_ts) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%S', user_sessions.session_end_ts) AS INTEGER) - CAST(STRFTIME('%S', user_sessions.session_start_ts) AS INTEGER)
  ) AS avg_session_duration_seconds
FROM main.user_sessions AS user_sessions
GROUP BY
  user_sessions.device_type

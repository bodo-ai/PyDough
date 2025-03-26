SELECT
  user_sessions.device_type AS device_type,
  AVG(
    DATEDIFF(
      CAST(user_sessions.session_end_ts AS DATETIME),
      CAST(user_sessions.session_start_ts AS DATETIME),
      SECOND
    )
  ) AS avg_session_duration_seconds
FROM main.user_sessions AS user_sessions
GROUP BY
  user_sessions.device_type

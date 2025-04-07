SELECT
  device_type,
  AVG(DATEDIFF(session_end_ts, session_start_ts, SECOND)) AS avg_session_duration_seconds
FROM main.user_sessions
GROUP BY
  device_type

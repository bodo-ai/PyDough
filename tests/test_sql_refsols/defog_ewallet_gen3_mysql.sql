SELECT
  device_type,
  AVG(TIMESTAMPDIFF(SECOND, session_start_ts, session_end_ts)) AS avg_session_duration_seconds
FROM main.user_sessions
GROUP BY
  1

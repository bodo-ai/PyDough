SELECT
  device_type,
  AVG(DATEDIFF(SECOND, session_start_ts, session_end_ts)) AS avg_session_duration_seconds
FROM MAIN.USER_SESSIONS
GROUP BY
  device_type

SELECT
  device_type,
  AVG((
    CAST(session_end_ts AS DATE) - CAST(session_start_ts AS DATE)
  ) * 86400) AS avg_session_duration_seconds
FROM MAIN.USER_SESSIONS
GROUP BY
  device_type

SELECT
  device_type,
  AVG(
    DATEDIFF(CAST(session_end_ts AS DATETIME), CAST(session_start_ts AS DATETIME), SECOND)
  ) AS avg_session_duration_seconds
FROM main.user_sessions
GROUP BY
  device_type

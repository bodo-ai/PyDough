SELECT
  device_type,
  AVG(
    DATEDIFF(SECOND, CAST(session_start_ts AS DATETIME), CAST(session_end_ts AS DATETIME))
  ) AS avg_session_duration_seconds
FROM main.user_sessions
GROUP BY
  1

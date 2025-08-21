SELECT
  device_type,
  AVG(
    DATEDIFF(SECOND, CAST(session_start_ts AS DATETIME), CAST(session_end_ts AS DATETIME))
  ) AS avg_session_duration_seconds
FROM MAIN.USER_SESSIONS
GROUP BY
  1

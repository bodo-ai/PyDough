SELECT
  device_type,
  AVG(
    CAST(EXTRACT(EPOCH FROM CAST(session_end_ts AS TIMESTAMP) - CAST(session_start_ts AS TIMESTAMP)) AS BIGINT)
  ) AS avg_session_duration_seconds
FROM main.user_sessions
GROUP BY
  device_type

SELECT
  device_type,
  AVG(
    DATE_DIFF('SECOND', CAST(session_start_ts AS TIMESTAMP), CAST(session_end_ts AS TIMESTAMP))
  ) AS avg_session_duration_seconds
FROM postgres.user_sessions
GROUP BY
  1

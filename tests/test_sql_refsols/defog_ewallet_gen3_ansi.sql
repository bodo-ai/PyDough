SELECT
  device_type,
  AVG(expr_1) AS avg_session_duration_seconds
FROM (
  SELECT
    DATEDIFF(session_end_ts, session_start_ts, SECOND) AS expr_1,
    device_type
  FROM (
    SELECT
      device_type,
      session_end_ts,
      session_start_ts
    FROM main.user_sessions
  )
)
GROUP BY
  device_type

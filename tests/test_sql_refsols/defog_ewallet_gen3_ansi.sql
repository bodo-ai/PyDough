SELECT
  device_type,
  AVG(DATEDIFF(session_end_ts, session_start_ts, SECOND)) AS avg_session_duration_seconds
FROM (
  SELECT
    device_type,
    session_end_ts,
    session_start_ts
  FROM main.user_sessions
) AS _t0
GROUP BY
  device_type

SELECT
  device_type,
  AVG(
    (
      (
        CAST((JULIANDAY(DATE(session_end_ts, 'start of day')) - JULIANDAY(DATE(session_start_ts, 'start of day'))) AS INTEGER) * 24 + CAST(STRFTIME('%H', session_end_ts) AS INTEGER) - CAST(STRFTIME('%H', session_start_ts) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%M', session_end_ts) AS INTEGER) - CAST(STRFTIME('%M', session_start_ts) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%S', session_end_ts) AS INTEGER) - CAST(STRFTIME('%S', session_start_ts) AS INTEGER)
  ) AS avg_session_duration_seconds
FROM (
  SELECT
    device_type,
    session_end_ts,
    session_start_ts
  FROM main.user_sessions
) AS _t0
GROUP BY
  device_type

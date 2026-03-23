SELECT
  device_type,
  AVG(
    DATE_DIFF(
      'SECOND',
      CAST(DATE_TRUNC('SECOND', session_start_ts) AS TIMESTAMP),
      CAST(DATE_TRUNC('SECOND', session_end_ts) AS TIMESTAMP)
    )
  ) AS avg_session_duration_seconds
FROM postgres.main.user_sessions
GROUP BY
  1

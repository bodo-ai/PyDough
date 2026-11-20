SELECT
  device_type,
  AVG(
    CAST(DATE_DIFF(
      'SECOND',
      CAST(DATE_TRUNC('SECOND', CAST(session_start_ts AS TIMESTAMP)) AS TIMESTAMP),
      CAST(DATE_TRUNC('SECOND', CAST(session_end_ts AS TIMESTAMP)) AS TIMESTAMP)
    ) AS DOUBLE)
  ) AS avg_session_duration_seconds
FROM mongo.defog.user_sessions
GROUP BY
  1

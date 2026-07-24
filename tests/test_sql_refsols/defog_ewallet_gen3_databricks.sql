SELECT
  device_type,
  AVG(
    (
      (
        DATEDIFF(DAY, CAST(session_start_ts AS DATE), CAST(session_end_ts AS DATE)) * 24 + EXTRACT(HOUR FROM CAST(session_end_ts AS TIMESTAMP)) - EXTRACT(HOUR FROM CAST(session_start_ts AS TIMESTAMP))
      ) * 60 + EXTRACT(MINUTE FROM CAST(session_end_ts AS TIMESTAMP)) - EXTRACT(MINUTE FROM CAST(session_start_ts AS TIMESTAMP))
    ) * 60 + EXTRACT(SECOND FROM CAST(session_end_ts AS TIMESTAMP)) - EXTRACT(SECOND FROM CAST(session_start_ts AS TIMESTAMP))
  ) AS avg_session_duration_seconds
FROM defog.ewallet.user_sessions
GROUP BY
  1

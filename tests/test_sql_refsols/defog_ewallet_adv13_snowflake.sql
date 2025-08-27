SELECT
  COUNT(*) AS TUC
FROM main.user_sessions
WHERE
  session_end_ts >= DATE_TRUNC('DAY', DATEADD(MONTH, -1, CURRENT_TIMESTAMP()))
  OR session_start_ts >= DATE_TRUNC('DAY', DATEADD(MONTH, -1, CURRENT_TIMESTAMP()))

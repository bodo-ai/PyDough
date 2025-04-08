SELECT
  COUNT() AS TUC
FROM main.user_sessions
WHERE
  session_start_ts >= DATE_TRUNC('DAY', DATE_ADD(CURRENT_TIMESTAMP(), -1, 'MONTH'))

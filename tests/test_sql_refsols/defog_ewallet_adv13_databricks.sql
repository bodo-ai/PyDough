SELECT
  COUNT(*) AS TUC
FROM defog.ewallet.user_sessions
WHERE
  session_end_ts >= DATE_TRUNC('DAY', ADD_MONTHS(CURRENT_TIMESTAMP(), -1))
  OR session_start_ts >= DATE_TRUNC('DAY', ADD_MONTHS(CURRENT_TIMESTAMP(), -1))

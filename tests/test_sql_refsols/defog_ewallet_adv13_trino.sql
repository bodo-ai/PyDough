SELECT
  COUNT(*) AS TUC
FROM mongo.defog.user_sessions
WHERE
  session_end_ts >= DATE_TRUNC('DAY', DATE_ADD('MONTH', -1, CURRENT_TIMESTAMP))
  OR session_start_ts >= DATE_TRUNC('DAY', DATE_ADD('MONTH', -1, CURRENT_TIMESTAMP))

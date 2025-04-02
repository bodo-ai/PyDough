SELECT
  COUNT() AS "TUC"
FROM "main"."user_sessions" AS "user_sessions"
WHERE
  "user_sessions"."session_start_ts" >= DATE(DATETIME('now', '-1 month'), 'start of day')

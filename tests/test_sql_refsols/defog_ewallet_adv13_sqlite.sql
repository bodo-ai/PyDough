SELECT
  COUNT() AS TUC
FROM (
  SELECT
    (NULL)
  FROM (
    SELECT
      session_start_ts
    FROM main.user_sessions
  )
  WHERE
    session_start_ts >= DATE(DATETIME('now', '-1 month'), 'start of day')
)

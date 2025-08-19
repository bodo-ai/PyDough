SELECT
  device_type,
  COUNT(*) AS count
FROM main.user_sessions
GROUP BY
  1
ORDER BY
  count DESC
LIMIT 2

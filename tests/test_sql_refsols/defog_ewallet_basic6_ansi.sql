SELECT
  device_type,
  COUNT(*) AS count
FROM main.user_sessions
GROUP BY
  device_type
ORDER BY
  count DESC
LIMIT 2

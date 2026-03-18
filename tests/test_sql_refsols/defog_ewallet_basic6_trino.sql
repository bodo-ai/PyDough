SELECT
  device_type,
  COUNT(*) AS count
FROM postgres.user_sessions
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT 2

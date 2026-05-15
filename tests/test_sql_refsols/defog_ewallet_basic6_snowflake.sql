SELECT
  device_type,
  COUNT(*) AS count
FROM ewallet.user_sessions
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST
LIMIT 2

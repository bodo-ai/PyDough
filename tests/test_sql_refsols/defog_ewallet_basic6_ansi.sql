WITH _t0 AS (
  SELECT
    COUNT() AS count,
    device_type
  FROM main.user_sessions
  GROUP BY
    device_type
)
SELECT
  device_type,
  count
FROM _t0
ORDER BY
  count DESC
LIMIT 2

WITH _t1 AS (
  SELECT
    COUNT() AS agg_0,
    device_type
  FROM main.user_sessions
  GROUP BY
    device_type
)
SELECT
  device_type,
  COALESCE(agg_0, 0) AS count
FROM _t1
ORDER BY
  count DESC
LIMIT 2

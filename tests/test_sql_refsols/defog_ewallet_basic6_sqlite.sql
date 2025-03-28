SELECT
  device_type,
  count
FROM (
  SELECT
    COALESCE(agg_0, 0) AS count,
    device_type
  FROM (
    SELECT
      COUNT() AS agg_0,
      device_type
    FROM (
      SELECT
        device_type
      FROM main.user_sessions
    )
    GROUP BY
      device_type
  )
)
ORDER BY
  count DESC
LIMIT 2

SELECT
  device_type,
  count
FROM (
  SELECT
    count,
    device_type,
    ordering_1
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS count,
      COALESCE(agg_0, 0) AS ordering_1,
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
    ordering_1 DESC
  LIMIT 2
)
ORDER BY
  ordering_1 DESC

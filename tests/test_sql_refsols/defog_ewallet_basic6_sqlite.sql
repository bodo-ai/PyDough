WITH _t2 AS (
  SELECT
    COUNT() AS agg_0,
    device_type AS device_type
  FROM main.user_sessions
  GROUP BY
    device_type
), _t0 AS (
  SELECT
    COALESCE(agg_0, 0) AS count,
    device_type AS device_type,
    COALESCE(agg_0, 0) AS ordering_1
  FROM _t2
  ORDER BY
    ordering_1 DESC
  LIMIT 2
)
SELECT
  device_type AS device_type,
  count AS count
FROM _t0
ORDER BY
  ordering_1 DESC

WITH _t1 AS (
  SELECT
    COUNT() AS agg_0,
    state
  FROM main.customers
  GROUP BY
    state
)
SELECT
  state,
  COALESCE(agg_0, 0) AS total_signups
FROM _t1
ORDER BY
  total_signups DESC
LIMIT 2

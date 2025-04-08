WITH _t2 AS (
  SELECT
    COUNT() AS agg_0,
    state
  FROM main.customers
  GROUP BY
    state
), _t0 AS (
  SELECT
    COALESCE(agg_0, 0) AS ordering_1,
    state,
    COALESCE(agg_0, 0) AS total_signups
  FROM _t2
  ORDER BY
    ordering_1 DESC
  LIMIT 2
)
SELECT
  state,
  total_signups
FROM _t0
ORDER BY
  ordering_1 DESC

WITH _t0 AS (
  SELECT
    COUNT() AS total_signups,
    state
  FROM main.customers
  GROUP BY
    state
)
SELECT
  state,
  total_signups
FROM _t0
ORDER BY
  total_signups DESC
LIMIT 2

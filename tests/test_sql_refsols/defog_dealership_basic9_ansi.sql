SELECT
  state,
  total_signups
FROM (
  SELECT
    ordering_1,
    state,
    total_signups
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS ordering_1,
      COALESCE(agg_0, 0) AS total_signups,
      state
    FROM (
      SELECT
        COUNT() AS agg_0,
        state
      FROM (
        SELECT
          state
        FROM main.customers
      )
      GROUP BY
        state
    )
  )
  ORDER BY
    ordering_1 DESC
  LIMIT 2
)
ORDER BY
  ordering_1 DESC

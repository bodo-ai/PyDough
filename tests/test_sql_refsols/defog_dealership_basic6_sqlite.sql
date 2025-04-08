SELECT
  state,
  unique_customers,
  total_revenue
FROM (
  SELECT
    ordering_2,
    state,
    total_revenue,
    unique_customers
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS ordering_2,
      COALESCE(agg_0, 0) AS total_revenue,
      agg_1 AS unique_customers,
      state
    FROM (
      SELECT
        COUNT(DISTINCT customer_id) AS agg_1,
        SUM(sale_price) AS agg_0,
        state
      FROM (
        SELECT
          customer_id,
          sale_price,
          state
        FROM (
          SELECT
            _id,
            state
          FROM main.customers
        )
        INNER JOIN (
          SELECT
            customer_id,
            sale_price
          FROM main.sales
        )
          ON _id = customer_id
      )
      GROUP BY
        state
    )
  )
  ORDER BY
    ordering_2 DESC
  LIMIT 5
)
ORDER BY
  ordering_2 DESC

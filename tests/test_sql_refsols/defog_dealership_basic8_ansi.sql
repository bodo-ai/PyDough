SELECT
  make,
  model,
  total_sales,
  total_revenue
FROM (
  SELECT
    make,
    model,
    ordering_2,
    total_revenue,
    total_sales
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS ordering_2,
      COALESCE(agg_0, 0) AS total_revenue,
      COALESCE(agg_1, 0) AS total_sales,
      make,
      model
    FROM (
      SELECT
        agg_0,
        agg_1,
        make,
        model
      FROM (
        SELECT
          _id,
          make,
          model
        FROM main.cars
      )
      LEFT JOIN (
        SELECT
          COUNT() AS agg_1,
          SUM(sale_price) AS agg_0,
          car_id
        FROM (
          SELECT
            car_id,
            sale_price
          FROM main.sales
        )
        GROUP BY
          car_id
      )
        ON _id = car_id
    )
  )
  ORDER BY
    ordering_2 DESC
  LIMIT 5
)
ORDER BY
  ordering_2 DESC

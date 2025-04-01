SELECT
  first_name,
  last_name,
  total_sales,
  num_sales,
  sales_rank
FROM (
  SELECT
    COALESCE(agg_1, 0) AS ordering_2,
    first_name,
    last_name,
    num_sales,
    sales_rank,
    total_sales
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS num_sales,
      COALESCE(agg_1, 0) AS total_sales,
      ROW_NUMBER() OVER (ORDER BY COALESCE(agg_1, 0) DESC NULLS FIRST) AS sales_rank,
      agg_1,
      first_name,
      last_name
    FROM (
      SELECT
        agg_0,
        agg_1,
        first_name,
        last_name
      FROM (
        SELECT
          _id,
          first_name,
          last_name
        FROM main.salespersons
      )
      INNER JOIN (
        SELECT
          COUNT(_id) AS agg_0,
          SUM(sale_price) AS agg_1,
          salesperson_id
        FROM (
          SELECT
            _id,
            sale_price,
            salesperson_id
          FROM main.sales
        )
        GROUP BY
          salesperson_id
      )
        ON _id = salesperson_id
    )
  )
)
ORDER BY
  ordering_2 DESC

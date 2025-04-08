SELECT
  first_name,
  last_name,
  total_sales,
  total_revenue
FROM (
  SELECT
    first_name,
    last_name,
    ordering_2,
    total_revenue,
    total_sales
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS total_revenue,
      COALESCE(agg_1, 0) AS ordering_2,
      COALESCE(agg_1, 0) AS total_sales,
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
          COUNT() AS agg_1,
          SUM(sale_price) AS agg_0,
          salesperson_id
        FROM (
          SELECT
            sale_price,
            salesperson_id
          FROM (
            SELECT
              sale_date,
              sale_price,
              salesperson_id
            FROM main.sales
          )
          WHERE
            CAST((JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(sale_date, 'start of day'))) AS INTEGER) <= 30
        )
        GROUP BY
          salesperson_id
      )
        ON _id = salesperson_id
    )
  )
  ORDER BY
    ordering_2 DESC
  LIMIT 5
)
ORDER BY
  ordering_2 DESC

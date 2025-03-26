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
      COALESCE(agg_0, 0) AS ordering_2,
      COALESCE(agg_0, 0) AS total_revenue,
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
      LEFT JOIN (
        SELECT
          COUNT(_id) AS agg_1,
          SUM(sale_price) AS agg_0,
          salesperson_id
        FROM (
          SELECT
            _id,
            sale_price,
            salesperson_id
          FROM (
            SELECT
              _id,
              sale_date,
              sale_price,
              salesperson_id
            FROM main.sales
          )
          WHERE
            (
              (
                CAST(STRFTIME('%Y', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%Y', sale_date) AS INTEGER)
              ) * 12 + CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%m', sale_date) AS INTEGER)
            ) < 3
        )
        GROUP BY
          salesperson_id
      )
        ON _id = salesperson_id
    )
  )
  ORDER BY
    ordering_2 DESC
  LIMIT 3
)
ORDER BY
  ordering_2 DESC

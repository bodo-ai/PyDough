SELECT
  first_name,
  last_name,
  ASP
FROM (
  SELECT
    ASP,
    first_name,
    last_name,
    ordering_1
  FROM (
    SELECT
      agg_0 AS ASP,
      agg_0 AS ordering_1,
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
        AVG(sale_price) AS agg_0,
        salesperson_id
      FROM (
        SELECT
          sale_price,
          salesperson_id
        FROM main.sales
      )
      GROUP BY
        salesperson_id
    )
      ON _id = salesperson_id
  )
  ORDER BY
    ordering_1 DESC
  LIMIT 3
)
ORDER BY
  ordering_1 DESC

SELECT
  _id,
  first_name,
  last_name,
  total
FROM (
  SELECT
    _id,
    first_name,
    last_name,
    ordering_1,
    total
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS ordering_1,
      COALESCE(agg_0, 0) AS total,
      _id,
      first_name,
      last_name
    FROM (
      SELECT
        _id,
        agg_0,
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
          SUM(sale_price) AS agg_0,
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
  )
  ORDER BY
    ordering_1 DESC
  LIMIT 5
)
ORDER BY
  ordering_1 DESC

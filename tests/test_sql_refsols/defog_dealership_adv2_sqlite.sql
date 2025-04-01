SELECT
  _id,
  first_name,
  last_name,
  num_sales
FROM (
  SELECT
    COALESCE(agg_0, 0) AS num_sales,
    COALESCE(agg_0, 0) AS ordering_1,
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
    INNER JOIN (
      SELECT
        COUNT(_id) AS agg_0,
        salesperson_id
      FROM (
        SELECT
          _id,
          salesperson_id
        FROM (
          SELECT
            _id,
            sale_date,
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
  ordering_1 DESC

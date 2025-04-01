SELECT
  COALESCE(agg_0, 0) AS num_sales,
  COALESCE(agg_1, 0) AS total_revenue
FROM (
  SELECT
    agg_0,
    agg_1
  FROM (
    SELECT
      _id
    FROM (
      SELECT
        _id,
        make
      FROM main.cars
    )
    WHERE
      LOWER(make) LIKE '%toyota%'
  )
  LEFT JOIN (
    SELECT
      COUNT(_id) AS agg_0,
      SUM(sale_price) AS agg_1,
      car_id
    FROM (
      SELECT
        _id,
        car_id,
        sale_price
      FROM (
        SELECT
          _id,
          car_id,
          sale_date,
          sale_price
        FROM main.sales
      )
      WHERE
        sale_date >= DATE_ADD(CURRENT_TIMESTAMP(), -30, 'DAY')
    )
    GROUP BY
      car_id
  )
    ON _id = car_id
)

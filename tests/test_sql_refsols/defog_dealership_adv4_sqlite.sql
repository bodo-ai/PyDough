SELECT
  COALESCE(agg_0, 0) AS num_sales,
  CASE WHEN COALESCE(agg_2, 0) > 0 THEN COALESCE(agg_1, 0) ELSE NULL END AS total_revenue
FROM (
  SELECT
    agg_0,
    agg_1,
    agg_2
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
      COUNT(sale_price) AS agg_2,
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
        sale_date >= DATETIME('now', '-30 day')
    )
    GROUP BY
      car_id
  )
    ON _id = car_id
)

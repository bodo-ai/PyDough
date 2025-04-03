SELECT
  make,
  model,
  num_sales
FROM (
  SELECT
    COALESCE(agg_0, 0) AS num_sales,
    COALESCE(agg_0, 0) AS ordering_1,
    make,
    model
  FROM (
    SELECT
      agg_0,
      make,
      model
    FROM (
      SELECT
        _id,
        make,
        model
      FROM (
        SELECT
          _id,
          make,
          model,
          vin_number
        FROM main.cars
      )
      WHERE
        vin_number LIKE '%m5%'
    )
    LEFT JOIN (
      SELECT
        COUNT(_id) AS agg_0,
        car_id
      FROM (
        SELECT
          _id,
          car_id
        FROM main.sales
      )
      GROUP BY
        car_id
    )
      ON _id = car_id
  )
)
ORDER BY
  ordering_1 DESC

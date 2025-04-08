SELECT
  make,
  model,
  COALESCE(agg_0, 0) AS num_sales
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
      COUNT() AS agg_0,
      car_id
    FROM (
      SELECT
        car_id
      FROM main.sales
    )
    GROUP BY
      car_id
  )
    ON _id = car_id
)

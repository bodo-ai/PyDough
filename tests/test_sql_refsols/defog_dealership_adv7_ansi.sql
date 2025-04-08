SELECT
  make,
  model,
  year,
  color,
  vin_number,
  agg_0 AS avg_sale_price
FROM (
  SELECT
    _id,
    color,
    make,
    model,
    vin_number,
    year
  FROM main.cars
  WHERE
    (
      LOWER(make) LIKE '%fordS%'
    ) OR (
      LOWER(model) LIKE '%mustang%'
    )
)
LEFT JOIN (
  SELECT
    AVG(sale_price) AS agg_0,
    car_id
  FROM (
    SELECT
      car_id,
      sale_price
    FROM main.sales
  )
  GROUP BY
    car_id
)
  ON _id = car_id

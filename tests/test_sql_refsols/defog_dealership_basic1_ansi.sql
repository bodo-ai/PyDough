SELECT
  _id,
  make,
  model,
  year
FROM (
  SELECT
    _id,
    make,
    model,
    year
  FROM main.cars
)
ANTI JOIN (
  SELECT
    car_id
  FROM main.sales
)
  ON _id = car_id

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
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM (
      SELECT
        car_id
      FROM main.sales
    )
    WHERE
      _id = car_id
  )

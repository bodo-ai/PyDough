SELECT
  id AS _id,
  make,
  model,
  year
FROM dealership.cars
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM dealership.sales
    WHERE
      cars.id = car_id
  )

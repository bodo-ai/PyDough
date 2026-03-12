SELECT
  _id,
  make,
  model,
  year
FROM main.cars
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.sales
    WHERE
      cars._id = car_id
  )

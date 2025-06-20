SELECT
  cars._id AS _id,
  cars.make AS make,
  cars.model AS model,
  cars.year AS year
FROM main.cars AS cars
JOIN main.sales AS sales
  ON cars._id = sales.car_id

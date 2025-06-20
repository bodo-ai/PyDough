SELECT
  cars._id,
  cars.make,
  cars.model,
  cars.year
FROM main.cars AS cars
JOIN main.sales AS sales
  ON cars._id = sales.car_id

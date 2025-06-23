WITH _u_0 AS (
  SELECT
    car_id AS _u_1
  FROM main.sales
  GROUP BY
    car_id
)
SELECT
  cars._id,
  cars.make,
  cars.model,
  cars.year
FROM main.cars AS cars
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = cars._id
WHERE
  _u_0._u_1 IS NULL

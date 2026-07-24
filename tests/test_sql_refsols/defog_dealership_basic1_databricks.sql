WITH _u_0 AS (
  SELECT
    car_id AS _u_1
  FROM defog.dealership.sales
  GROUP BY
    1
)
SELECT
  cars.id AS _id,
  cars.make,
  cars.model,
  cars.year
FROM defog.dealership.cars AS cars
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = cars.id
WHERE
  _u_0._u_1 IS NULL

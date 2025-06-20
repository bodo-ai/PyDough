WITH _u_0 AS (
  SELECT
    sales.car_id AS _u_1
  FROM main.sales AS sales
  GROUP BY
    sales.car_id
)
SELECT
  cars._id AS _id,
  cars.make AS make,
  cars.model AS model,
  cars.year AS year
FROM main.cars AS cars
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = cars._id
WHERE
  _u_0._u_1 IS NULL

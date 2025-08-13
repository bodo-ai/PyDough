WITH _u_0 AS (
  SELECT
    car_id AS _u_1
  FROM MAIN.SALES
  GROUP BY
    car_id
)
SELECT
  CARS._id,
  CARS.make,
  CARS.model,
  CARS.year
FROM MAIN.CARS AS CARS
LEFT JOIN _u_0 AS _u_0
  ON CARS._id = _u_0._u_1
WHERE
  _u_0._u_1 IS NULL

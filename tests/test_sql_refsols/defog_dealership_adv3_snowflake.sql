WITH _S1 AS (
  SELECT
    COUNT(*) AS AGG_0,
    car_id AS CAR_ID
  FROM MAIN.SALES
  GROUP BY
    car_id
)
SELECT
  CARS.make,
  CARS.model,
  COALESCE(_S1.AGG_0, 0) AS num_sales
FROM MAIN.CARS AS CARS
LEFT JOIN _S1 AS _S1
  ON CARS._id = _S1.CAR_ID
WHERE
  LOWER(CARS.vin_number) LIKE '%m5%'

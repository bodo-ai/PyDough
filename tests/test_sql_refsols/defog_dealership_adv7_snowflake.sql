WITH _S1 AS (
  SELECT
    AVG(sale_price) AS AVG_SALE_PRICE,
    car_id AS CAR_ID
  FROM MAIN.SALES
  GROUP BY
    car_id
)
SELECT
  CARS.make,
  CARS.model,
  CARS.year,
  CARS.color,
  CARS.vin_number,
  _S1.AVG_SALE_PRICE AS avg_sale_price
FROM MAIN.CARS AS CARS
LEFT JOIN _S1 AS _S1
  ON CARS._id = _S1.CAR_ID
WHERE
  CONTAINS(LOWER(CARS.make), 'fords') OR CONTAINS(LOWER(CARS.model), 'mustang')

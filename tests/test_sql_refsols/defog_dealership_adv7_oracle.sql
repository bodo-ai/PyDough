WITH _S1 AS (
  SELECT
    car_id AS CAR_ID,
    AVG(sale_price) AS AVG_SALE_PRICE
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
FROM MAIN.CARS CARS
LEFT JOIN _S1 _S1
  ON CARS._id = _S1.CAR_ID
WHERE
  LOWER(CARS.make) LIKE '%fords%' OR LOWER(CARS.model) LIKE '%mustang%'

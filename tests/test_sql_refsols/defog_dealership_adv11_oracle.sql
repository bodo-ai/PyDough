WITH "_S0" AS (
  SELECT
    car_id AS CAR_ID,
    SUM(sale_price) AS SUM_SALE_PRICE
  FROM MAIN.SALES
  WHERE
    EXTRACT(YEAR FROM CAST(sale_date AS DATE)) = 2023
  GROUP BY
    car_id
)
SELECT
  (
    (
      COALESCE(SUM("_S0".SUM_SALE_PRICE), 0) - COALESCE(SUM(CARS.cost), 0)
    ) / NULLIF(SUM(CARS.cost), 0)
  ) * 100 AS GPM
FROM "_S0" "_S0"
JOIN MAIN.CARS CARS
  ON CARS."_id" = "_S0".CAR_ID

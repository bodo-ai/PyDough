WITH _S0 AS (
  SELECT
    SUM(sale_price) AS AGG_0,
    car_id AS CAR_ID
  FROM MAIN.SALES
  WHERE
    DATE_PART(YEAR, sale_date) = 2023
  GROUP BY
    car_id
), _T0 AS (
  SELECT
    SUM(_S0.AGG_0) AS AGG_0,
    SUM(CARS.cost) AS AGG_1
  FROM _S0 AS _S0
  JOIN MAIN.CARS AS CARS
    ON CARS._id = _S0.CAR_ID
)
SELECT
  (
    (
      COALESCE(AGG_0, 0) - COALESCE(AGG_1, 0)
    ) / COALESCE(AGG_1, 0)
  ) * 100 AS GPM
FROM _T0

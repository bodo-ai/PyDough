WITH _s0 AS (
  SELECT
    car_id,
    SUM(sale_price) AS sum_sale_price
  FROM main.sales
  WHERE
    EXTRACT(YEAR FROM CAST(sale_date AS DATETIME)) = 2023
  GROUP BY
    1
)
SELECT
  (
    (
      COALESCE(SUM(_s0.sum_sale_price), 0) - COALESCE(SUM(cars.cost), 0)
    ) / COALESCE(SUM(cars.cost), 0)
  ) * 100 AS GPM
FROM _s0 AS _s0
JOIN main.cars AS cars
  ON _s0.car_id = cars._id

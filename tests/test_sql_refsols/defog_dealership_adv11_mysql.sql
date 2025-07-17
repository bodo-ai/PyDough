WITH _s0 AS (
  SELECT
    SUM(sale_price) AS sum_sale_price,
    car_id
  FROM main.sales
  WHERE
    YEAR(sale_date) = 2023
  GROUP BY
    car_id
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

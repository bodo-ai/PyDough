WITH _s0 AS (
  SELECT
    car_id,
    SUM(sale_price) AS sum_sale_price
  FROM main.sales
  WHERE
    CAST(STRFTIME('%Y', sale_date) AS INTEGER) = 2023
  GROUP BY
    1
)
SELECT
  (
    CAST((
      COALESCE(SUM(_s0.sum_sale_price), 0) - COALESCE(SUM(cars.cost), 0)
    ) AS REAL) / COALESCE(SUM(cars.cost), 0)
  ) * 100 AS GPM
FROM _s0 AS _s0
JOIN main.cars AS cars
  ON _s0.car_id = cars._id

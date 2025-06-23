WITH _s0 AS (
  SELECT
    SUM(sale_price) AS sum_sale_price,
    car_id
  FROM main.sales
  WHERE
    CAST(STRFTIME('%Y', sale_date) AS INTEGER) = 2023
  GROUP BY
    car_id
), _t0 AS (
  SELECT
    SUM(cars.cost) AS sum_cost,
    SUM(_s0.sum_sale_price) AS sum_sum_sale_price
  FROM _s0 AS _s0
  JOIN main.cars AS cars
    ON _s0.car_id = cars._id
)
SELECT
  (
    CAST((
      COALESCE(sum_sum_sale_price, 0) - COALESCE(sum_cost, 0)
    ) AS REAL) / COALESCE(sum_cost, 0)
  ) * 100 AS GPM
FROM _t0

WITH _t1 AS (
  SELECT
    ANY_VALUE(cars.cost) AS anything_cost,
    SUM(sales.sale_price) AS sum_sale_price
  FROM main.sales AS sales
  JOIN main.cars AS cars
    ON cars._id = sales.car_id
  WHERE
    YEAR(CAST(sales.sale_date AS TIMESTAMP)) = 2023
  GROUP BY
    sales.car_id
)
SELECT
  (
    (
      COALESCE(SUM(sum_sale_price), 0) - COALESCE(SUM(anything_cost), 0)
    ) / COALESCE(SUM(anything_cost), 0)
  ) * 100 AS GPM
FROM _t1

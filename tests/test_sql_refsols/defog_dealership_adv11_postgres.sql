WITH _t1 AS (
  SELECT
    MAX(cars.cost) AS anything_cost,
    SUM(sales.sale_price) AS sum_sale_price
  FROM main.sales AS sales
  JOIN main.cars AS cars
    ON cars._id = sales.car_id
  WHERE
    EXTRACT(YEAR FROM CAST(sales.sale_date AS TIMESTAMP)) = 2023
  GROUP BY
    sales.car_id
)
SELECT
  (
    CAST((
      COALESCE(SUM(sum_sale_price), 0) - COALESCE(SUM(anything_cost), 0)
    ) AS DOUBLE PRECISION) / COALESCE(SUM(anything_cost), 0)
  ) * 100 AS GPM
FROM _t1

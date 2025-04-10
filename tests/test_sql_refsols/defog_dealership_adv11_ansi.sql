WITH _t0 AS (
  SELECT
    SUM(cars.cost) AS agg_1,
    SUM(sales.sale_price) AS agg_0
  FROM main.sales AS sales
  JOIN main.cars AS cars
    ON cars._id = sales.car_id
  WHERE
    EXTRACT(YEAR FROM sales.sale_date) = 2023
)
SELECT
  (
    (
      COALESCE(agg_0, 0) - COALESCE(agg_1, 0)
    ) / COALESCE(agg_1, 0)
  ) * 100 AS GPM
FROM _t0

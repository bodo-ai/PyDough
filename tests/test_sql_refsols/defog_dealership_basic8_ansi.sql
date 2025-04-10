WITH _t1 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(sale_price) AS agg_0,
    car_id
  FROM main.sales
  GROUP BY
    car_id
), _t0_2 AS (
  SELECT
    cars.make,
    cars.model,
    COALESCE(_t1.agg_0, 0) AS ordering_2,
    COALESCE(_t1.agg_0, 0) AS total_revenue,
    COALESCE(_t1.agg_1, 0) AS total_sales
  FROM main.cars AS cars
  LEFT JOIN _t1 AS _t1
    ON _t1.car_id = cars._id
  ORDER BY
    ordering_2 DESC
  LIMIT 5
)
SELECT
  make,
  model,
  total_sales,
  total_revenue
FROM _t0_2
ORDER BY
  ordering_2 DESC

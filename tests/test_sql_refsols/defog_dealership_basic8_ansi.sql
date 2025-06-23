WITH _s1 AS (
  SELECT
    COUNT(*) AS agg_1,
    SUM(sale_price) AS sum_sale_price,
    car_id
  FROM main.sales
  GROUP BY
    car_id
)
SELECT
  cars.make,
  cars.model,
  COALESCE(_s1.agg_1, 0) AS total_sales,
  COALESCE(_s1.sum_sale_price, 0) AS total_revenue
FROM main.cars AS cars
LEFT JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
ORDER BY
  total_revenue DESC
LIMIT 5

WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sale_price) AS sum_sale_price,
    car_id
  FROM main.sales
  GROUP BY
    3
)
SELECT
  cars.make,
  cars.model,
  COALESCE(_s1.n_rows, 0) AS total_sales,
  COALESCE(_s1.sum_sale_price, 0) AS total_revenue
FROM main.cars AS cars
LEFT JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
ORDER BY
  4 DESC NULLS LAST
LIMIT 5

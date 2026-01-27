WITH _s1 AS (
  SELECT
    car_id,
    COUNT(*) AS n_rows,
    SUM(sale_price) AS sum_sale_price
  FROM dealership.sales
  GROUP BY
    1
)
SELECT
  cars.make,
  cars.model,
  COALESCE(_s1.n_rows, 0) AS total_sales,
  COALESCE(_s1.sum_sale_price, 0) AS total_revenue
FROM dealership.cars AS cars
LEFT JOIN _s1 AS _s1
  ON _s1.car_id = cars.id
ORDER BY
  4 DESC NULLS LAST
LIMIT 5

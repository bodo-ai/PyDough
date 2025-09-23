WITH _s1 AS (
  SELECT
    car_id,
    COUNT(*) AS n_rows,
    SUM(sale_price) AS sum_sale_price
  FROM main.sales
  GROUP BY
    1
)
SELECT
  cars.make,
  cars.model,
  _s1.n_rows AS total_sales,
  COALESCE(_s1.sum_sale_price, 0) AS total_revenue
FROM main.cars AS cars
JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
ORDER BY
  4 DESC
LIMIT 5

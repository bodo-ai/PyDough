WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sale_price) AS sum_sale_price,
    car_id
  FROM main.sales
  GROUP BY
    car_id
), _t0 AS (
  SELECT
    cars.make AS make_1,
    cars.model AS model_1,
    COALESCE(_s1.sum_sale_price, 0) AS total_revenue_1,
    _s1.n_rows
  FROM main.cars AS cars
  LEFT JOIN _s1 AS _s1
    ON _s1.car_id = cars._id
  ORDER BY
    COALESCE(_s1.sum_sale_price, 0) DESC
  LIMIT 5
)
SELECT
  make_1 AS make,
  model_1 AS model,
  COALESCE(n_rows, 0) AS total_sales,
  total_revenue_1 AS total_revenue
FROM _t0
ORDER BY
  total_revenue_1 DESC

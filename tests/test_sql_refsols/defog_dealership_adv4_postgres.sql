WITH _s1 AS (
  SELECT
    car_id,
    COUNT(*) AS n_rows,
    SUM(sale_price) AS sum_sale_price
  FROM main.sales
  WHERE
    sale_date >= CURRENT_TIMESTAMP - INTERVAL '30 DAY'
  GROUP BY
    1
)
SELECT
  COALESCE(_s1.n_rows, 0) AS num_sales,
  CASE
    WHEN (
      NOT _s1.n_rows IS NULL AND _s1.n_rows > 0
    )
    THEN COALESCE(_s1.sum_sale_price, 0)
    ELSE NULL
  END AS total_revenue
FROM main.cars AS cars
LEFT JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
WHERE
  LOWER(cars.make) LIKE '%toyota%'

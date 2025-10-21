WITH _s1 AS (
  SELECT
    car_id,
    sale_price
  FROM main.sales
  WHERE
    sale_date >= DATETIME('now', '-30 day')
), _t0 AS (
  SELECT
    _s1.car_id,
    COUNT(*) AS n_rows,
    SUM(_s1.sale_price) AS sum_sale_price
  FROM main.cars AS cars
  LEFT JOIN _s1 AS _s1
    ON _s1.car_id = cars._id
  WHERE
    LOWER(cars.make) LIKE '%toyota%'
  GROUP BY
    1
)
SELECT
  n_rows * IIF(NOT car_id IS NULL, 1, 0) AS num_sales,
  CASE
    WHEN (
      n_rows * IIF(NOT car_id IS NULL, 1, 0)
    ) > 0
    THEN COALESCE(sum_sale_price, 0)
    ELSE NULL
  END AS total_revenue
FROM _t0

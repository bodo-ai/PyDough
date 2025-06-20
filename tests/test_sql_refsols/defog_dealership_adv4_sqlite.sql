WITH _s1 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM(sale_price) AS agg_1,
    car_id
  FROM main.sales
  WHERE
    sale_date >= DATETIME('now', '-30 day')
  GROUP BY
    car_id
)
SELECT
  COALESCE(_s1.agg_0, 0) AS num_sales,
  CASE
    WHEN (
      NOT _s1.agg_0 IS NULL AND _s1.agg_0 > 0
    )
    THEN COALESCE(_s1.agg_1, 0)
    ELSE NULL
  END AS total_revenue
FROM main.cars AS cars
LEFT JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
WHERE
  LOWER(cars.make) LIKE '%toyota%'

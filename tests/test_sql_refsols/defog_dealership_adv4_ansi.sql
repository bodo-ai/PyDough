WITH _t1_2 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(sale_price) AS agg_1,
    car_id
  FROM main.sales
  WHERE
    sale_date >= DATE_ADD(CURRENT_TIMESTAMP(), -30, 'DAY')
  GROUP BY
    car_id
)
SELECT
  COALESCE(_t1.agg_0, 0) AS num_sales,
  CASE
    WHEN (
      NOT _t1.agg_0 IS NULL AND _t1.agg_0 > 0
    )
    THEN COALESCE(_t1.agg_1, 0)
    ELSE NULL
  END AS total_revenue
FROM main.cars AS cars
LEFT JOIN _t1_2 AS _t1
  ON _t1.car_id = cars._id
WHERE
  LOWER(cars.make) LIKE '%toyota%'

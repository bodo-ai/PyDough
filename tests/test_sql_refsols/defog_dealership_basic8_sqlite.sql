WITH _s3 AS (
  SELECT
    SUM(sale_price) AS agg_0,
    COUNT(*) AS agg_1,
    car_id
  FROM main.sales
  GROUP BY
    car_id
)
SELECT
  _s0.make,
  _s0.model,
  COALESCE(_s3.agg_1, 0) AS total_sales,
  COALESCE(_s3.agg_0, 0) AS total_revenue
FROM main.cars AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0._id = _s3.car_id
ORDER BY
  total_revenue DESC
LIMIT 5

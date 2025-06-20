WITH _s3 AS (
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
  COALESCE(_s3.agg_0, 0) AS num_sales,
  CASE
    WHEN (
      NOT _s3.agg_0 IS NULL AND _s3.agg_0 > 0
    )
    THEN COALESCE(_s3.agg_1, 0)
    ELSE NULL
  END AS total_revenue
FROM main.cars AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0._id = _s3.car_id
WHERE
  LOWER(_s0.make) LIKE '%toyota%'

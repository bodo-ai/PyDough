WITH _s3 AS (
  SELECT
    COUNT(*) AS agg_0,
    car_id
  FROM main.sales
  GROUP BY
    car_id
)
SELECT
  _s0.make,
  _s0.model,
  COALESCE(_s3.agg_0, 0) AS num_sales
FROM main.cars AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0._id = _s3.car_id
WHERE
  LOWER(_s0.vin_number) LIKE '%m5%'

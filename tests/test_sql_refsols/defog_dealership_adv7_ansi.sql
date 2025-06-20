WITH _s3 AS (
  SELECT
    AVG(sale_price) AS agg_0,
    car_id
  FROM main.sales
  GROUP BY
    car_id
)
SELECT
  _s0.make,
  _s0.model,
  _s0.year,
  _s0.color,
  _s0.vin_number,
  _s3.agg_0 AS avg_sale_price
FROM main.cars AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0._id = _s3.car_id
WHERE
  LOWER(_s0.make) LIKE '%fords%' OR LOWER(_s0.model) LIKE '%mustang%'

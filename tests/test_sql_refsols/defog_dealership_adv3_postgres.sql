WITH _s2 AS (
  SELECT DISTINCT
    make,
    model
  FROM main.cars
  WHERE
    LOWER(vin_number) LIKE '%m5%'
), _s3 AS (
  SELECT
    cars.make,
    cars.model,
    COUNT(*) AS n_rows
  FROM main.cars AS cars
  JOIN main.sales AS sales
    ON cars._id = sales.car_id
  WHERE
    LOWER(cars.vin_number) LIKE '%m5%'
  GROUP BY
    1,
    2
)
SELECT
  _s2.make,
  _s2.model,
  COALESCE(_s3.n_rows, 0) AS num_sales
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.make = _s3.make AND _s2.model = _s3.model

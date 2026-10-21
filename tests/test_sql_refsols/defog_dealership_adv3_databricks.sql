WITH _s2 AS (
  SELECT DISTINCT
    make,
    model
  FROM defog.dealership.cars
  WHERE
    CONTAINS(LOWER(vin_number), 'm5')
), _s1 AS (
  SELECT
    car_id,
    COUNT(*) AS n_rows
  FROM defog.dealership.sales
  GROUP BY
    1
), _s3 AS (
  SELECT
    cars.make,
    cars.model,
    SUM(_s1.n_rows) AS sum_n_rows
  FROM defog.dealership.cars AS cars
  JOIN _s1 AS _s1
    ON _s1.car_id = cars.id
  WHERE
    CONTAINS(LOWER(cars.vin_number), 'm5')
  GROUP BY
    1,
    2
)
SELECT
  _s2.make,
  _s2.model,
  COALESCE(_s3.sum_n_rows, 0) AS num_sales
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.make = _s3.make AND _s2.model = _s3.model

WITH _s0 AS (
  SELECT
    SUM(sale_price) AS agg_0,
    car_id
  FROM main.sales
  WHERE
    EXTRACT(YEAR FROM sale_date) = 2023
  GROUP BY
    car_id
), _t0 AS (
  SELECT
    SUM(_s0.agg_0) AS agg_0,
    SUM(cars.cost) AS agg_1
  FROM _s0 AS _s0
  JOIN main.cars AS cars
    ON _s0.car_id = cars._id
)
SELECT
  (
    (
      COALESCE(agg_0, 0) - COALESCE(agg_1, 0)
    ) / COALESCE(agg_1, 0)
  ) * 100 AS GPM
FROM _t0

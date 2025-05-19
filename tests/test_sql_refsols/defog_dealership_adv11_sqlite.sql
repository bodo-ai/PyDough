WITH _t0 AS (
  SELECT
    SUM(sales.sale_price) AS agg_0,
    SUM(cars.cost) AS agg_1
  FROM main.sales AS sales
  JOIN main.cars AS cars
    ON cars._id = sales.car_id
  WHERE
    CAST(STRFTIME('%Y', sales.sale_date) AS INTEGER) = 2023
)
SELECT
  (
    CAST((
      COALESCE(agg_0, 0) - COALESCE(agg_1, 0)
    ) AS REAL) / COALESCE(agg_1, 0)
  ) * 100 AS GPM
FROM _t0

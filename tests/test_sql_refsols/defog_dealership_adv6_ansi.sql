WITH _t1 AS (
  SELECT
    car_id
  FROM main.inventory_snapshots
  QUALIFY
    NOT is_in_inventory
    AND ROW_NUMBER() OVER (PARTITION BY car_id ORDER BY snapshot_date DESC NULLS FIRST) = 1
), _s3 AS (
  SELECT
    MAX(sale_price) AS agg_0,
    car_id
  FROM main.sales
  GROUP BY
    car_id
)
SELECT
  cars.make,
  cars.model,
  _s3.agg_0 AS highest_sale_price
FROM main.cars AS cars
JOIN _t1 AS _t1
  ON _t1.car_id = cars._id
LEFT JOIN _s3 AS _s3
  ON _s3.car_id = cars._id
ORDER BY
  _s3.agg_0 DESC

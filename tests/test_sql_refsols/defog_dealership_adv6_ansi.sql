WITH _t1 AS (
  SELECT
    cars._id,
    cars.make,
    cars.model
  FROM main.cars AS cars
  JOIN main.inventory_snapshots AS inventory_snapshots
    ON cars._id = inventory_snapshots.car_id
  QUALIFY
    NOT inventory_snapshots.is_in_inventory
    AND ROW_NUMBER() OVER (PARTITION BY cars._id ORDER BY inventory_snapshots.snapshot_date DESC NULLS FIRST) = 1
), _s3 AS (
  SELECT
    MAX(sale_price) AS agg_0,
    car_id
  FROM main.sales
  GROUP BY
    car_id
)
SELECT
  _t1.make,
  _t1.model,
  _s3.agg_0 AS highest_sale_price
FROM _t1 AS _t1
LEFT JOIN _s3 AS _s3
  ON _s3.car_id = _t1._id
ORDER BY
  _s3.agg_0 DESC

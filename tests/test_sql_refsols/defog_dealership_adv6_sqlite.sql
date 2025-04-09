WITH _t AS (
  SELECT
    cars._id,
    inventory_snapshots.is_in_inventory,
    cars.make,
    cars.model,
    ROW_NUMBER() OVER (PARTITION BY cars._id ORDER BY inventory_snapshots.snapshot_date DESC) AS _w
  FROM main.cars AS cars
  JOIN main.inventory_snapshots AS inventory_snapshots
    ON cars._id = inventory_snapshots.car_id
), _s3 AS (
  SELECT
    MAX(sale_price) AS agg_0,
    car_id
  FROM main.sales
  GROUP BY
    car_id
)
SELECT
  _t.make,
  _t.model,
  _s3.agg_0 AS highest_sale_price
FROM _t AS _t
LEFT JOIN _s3 AS _s3
  ON _s3.car_id = _t._id
WHERE
  NOT _t.is_in_inventory AND _t._w = 1
ORDER BY
  _s3.agg_0 DESC

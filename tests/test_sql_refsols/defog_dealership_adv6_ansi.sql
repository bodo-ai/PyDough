WITH _t1_2 AS (
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
), _t3_2 AS (
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
  _t3.agg_0 AS highest_sale_price
FROM _t1_2 AS _t1
LEFT JOIN _t3_2 AS _t3
  ON _t1._id = _t3.car_id
ORDER BY
  _t3.agg_0 DESC

WITH _t0 AS (
  SELECT
    car_id
  FROM main.inventory_snapshots
  WHERE
    snapshot_date <= '2023-03-31' AND snapshot_date >= '2023-03-01'
  QUALIFY
    RANK() OVER (ORDER BY snapshot_date DESC NULLS FIRST) = 1 AND is_in_inventory
)
SELECT
  cars._id,
  cars.make,
  cars.model,
  cars.year
FROM _t0 AS _t0
JOIN main.cars AS cars
  ON _t0.car_id = cars._id

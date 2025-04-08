WITH _t AS (
  SELECT
    car_id,
    is_in_inventory,
    RANK() OVER (ORDER BY snapshot_date DESC) AS _w
  FROM main.inventory_snapshots
  WHERE
    snapshot_date <= '2023-03-31' AND snapshot_date >= '2023-03-01'
)
SELECT
  cars._id,
  cars.make,
  cars.model,
  cars.year
FROM _t AS _t
JOIN main.cars AS cars
  ON _t.car_id = cars._id
WHERE
  _t._w = 1 AND _t.is_in_inventory

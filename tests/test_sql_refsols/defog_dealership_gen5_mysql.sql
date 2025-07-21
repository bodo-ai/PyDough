WITH _t AS (
  SELECT
    car_id,
    is_in_inventory,
    RANK() OVER (ORDER BY CASE WHEN snapshot_date IS NULL THEN 1 ELSE 0 END DESC, snapshot_date DESC) AS _w
  FROM main.inventory_snapshots
  WHERE
    MONTH(snapshot_date) = 3 AND YEAR(snapshot_date) = 2023
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

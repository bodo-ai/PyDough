WITH _t AS (
  SELECT
    car_id,
    is_in_inventory,
    RANK() OVER (ORDER BY snapshot_date DESC NULLS FIRST) AS _w
  FROM postgres.main.inventory_snapshots
  WHERE
    MONTH(CAST(snapshot_date AS TIMESTAMP)) = 3
    AND YEAR(CAST(snapshot_date AS TIMESTAMP)) = 2023
)
SELECT
  cars._id,
  cars.make,
  cars.model,
  cars.year
FROM _t AS _t
JOIN postgres.main.cars AS cars
  ON _t.car_id = cars._id
WHERE
  _t._w = 1 AND _t.is_in_inventory

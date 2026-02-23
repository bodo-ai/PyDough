WITH _t0 AS (
  SELECT
    car_id
  FROM dealership.inventory_snapshots
  WHERE
    MONTH(CAST(snapshot_date AS TIMESTAMP)) = 3
    AND YEAR(CAST(snapshot_date AS TIMESTAMP)) = 2023
  QUALIFY
    RANK() OVER (ORDER BY snapshot_date DESC) = 1 AND is_in_inventory
)
SELECT
  cars.id AS _id,
  cars.make,
  cars.model,
  cars.year
FROM _t0 AS _t0
JOIN dealership.cars AS cars
  ON _t0.car_id = cars.id

WITH _t0 AS (
  SELECT
    car_id
  FROM defog.dealership.inventory_snapshots
  WHERE
    EXTRACT(MONTH FROM CAST(snapshot_date AS TIMESTAMP)) = 3
    AND EXTRACT(YEAR FROM CAST(snapshot_date AS TIMESTAMP)) = 2023
  QUALIFY
    RANK() OVER (ORDER BY snapshot_date DESC NULLS FIRST) = 1 AND is_in_inventory
)
SELECT
  cars.id AS _id,
  cars.make,
  cars.model,
  cars.year
FROM _t0 AS _t0
JOIN defog.dealership.cars AS cars
  ON _t0.car_id = cars.id

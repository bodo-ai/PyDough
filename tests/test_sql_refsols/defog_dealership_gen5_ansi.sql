WITH _t0 AS (
  SELECT
    car_id
  FROM main.inventory_snapshots
  WHERE
    EXTRACT(MONTH FROM snapshot_date) = 3 AND EXTRACT(YEAR FROM snapshot_date) = 2023
  QUALIFY
    RANK() OVER (ORDER BY snapshot_date DESC NULLS FIRST) = 1 AND is_in_inventory
)
SELECT
  _s1._id,
  _s1.make,
  _s1.model,
  _s1.year
FROM _t0 AS _t0
JOIN main.cars AS _s1
  ON _s1._id = _t0.car_id

WITH _t AS (
  SELECT
    car_id,
    is_in_inventory,
    RANK() OVER (ORDER BY snapshot_date DESC) AS _w
  FROM main.inventory_snapshots
  WHERE
    CAST(STRFTIME('%Y', snapshot_date) AS INTEGER) = 2023
    AND CAST(STRFTIME('%m', snapshot_date) AS INTEGER) = 3
)
SELECT
  _s1._id,
  _s1.make,
  _s1.model,
  _s1.year
FROM _t AS _t
JOIN main.cars AS _s1
  ON _s1._id = _t.car_id
WHERE
  _t._w = 1 AND _t.is_in_inventory

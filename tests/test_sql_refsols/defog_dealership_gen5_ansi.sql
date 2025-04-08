SELECT
  _id AS _id,
  make,
  model,
  year
FROM (
  SELECT
    car_id
  FROM (
    SELECT
      *
    FROM (
      SELECT
        car_id,
        is_in_inventory,
        snapshot_date
      FROM main.inventory_snapshots
      WHERE
        (
          snapshot_date <= '2023-03-31'
        ) AND (
          snapshot_date >= '2023-03-01'
        )
    )
    QUALIFY
      (
        RANK() OVER (ORDER BY snapshot_date DESC NULLS FIRST) = 1
      )
      AND is_in_inventory
  )
)
INNER JOIN (
  SELECT
    _id,
    make,
    model,
    year
  FROM main.cars
)
  ON car_id = _id

SELECT
  _id,
  make,
  model,
  year
FROM (
  SELECT
    _id,
    last_snapshot_date,
    make,
    model,
    snapshot_date,
    year
  FROM (
    SELECT
      _id,
      last_snapshot_date,
      make,
      model,
      year
    FROM (
      SELECT
        MAX(snapshot_date) AS last_snapshot_date
      FROM (
        SELECT
          snapshot_date
        FROM main.inventory_snapshots
        WHERE
          (
            snapshot_date <= '2023-03-31'
          ) AND (
            snapshot_date >= '2023-03-01'
          )
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
      ON TRUE
  )
  INNER JOIN (
    SELECT
      car_id,
      snapshot_date
    FROM (
      SELECT
        car_id,
        is_in_inventory,
        snapshot_date
      FROM main.inventory_snapshots
    )
    WHERE
      is_in_inventory = 1
  )
    ON _id = car_id
)
WHERE
  snapshot_date = last_snapshot_date

SELECT
  make_19 AS make,
  model_20 AS model,
  sale_price
FROM (
  SELECT
    make_17 AS make_19,
    model_18 AS model_20,
    ordering_0,
    sale_price
  FROM (
    SELECT
      make AS make_17,
      model AS model_18,
      sale_price AS ordering_0,
      sale_price
    FROM (
      SELECT
        car_id_4,
        sale_price
      FROM (
        SELECT
          car_id AS car_id_4,
          sale_date,
          sale_price,
          snapshot_date
        FROM (
          SELECT
            _id AS _id_1,
            sale_date,
            sale_price
          FROM (
            SELECT
              car_id,
              sale_date,
              sale_price
            FROM main.sales
          )
          INNER JOIN (
            SELECT
              _id
            FROM main.cars
          )
            ON car_id = _id
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
            is_in_inventory = 0
        )
          ON _id_1 = car_id
      )
      WHERE
        snapshot_date = sale_date
    )
    INNER JOIN (
      SELECT
        _id,
        make,
        model
      FROM main.cars
    )
      ON car_id_4 = _id
  )
  ORDER BY
    ordering_0 DESC
  LIMIT 1
)
ORDER BY
  ordering_0 DESC

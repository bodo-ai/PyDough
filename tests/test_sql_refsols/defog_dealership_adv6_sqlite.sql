SELECT
  make,
  model,
  highest_sale_price
FROM (
  SELECT
    agg_0 AS highest_sale_price,
    agg_0 AS ordering_1,
    make,
    model
  FROM (
    SELECT
      _id,
      make,
      model
    FROM (
      SELECT
        *
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY _id ORDER BY snapshot_date DESC) AS _w
        FROM (
          SELECT
            _id,
            is_in_inventory,
            make,
            model,
            snapshot_date
          FROM (
            SELECT
              _id,
              make,
              model
            FROM main.cars
          )
          INNER JOIN (
            SELECT
              car_id,
              is_in_inventory,
              snapshot_date
            FROM main.inventory_snapshots
          )
            ON _id = car_id
        )
      ) AS _t
      WHERE
        (
          _w = 1
        ) AND (
          NOT is_in_inventory
        )
    )
  )
  LEFT JOIN (
    SELECT
      MAX(sale_price) AS agg_0,
      car_id
    FROM (
      SELECT
        car_id,
        sale_price
      FROM main.sales
    )
    GROUP BY
      car_id
  )
    ON _id = car_id
)
ORDER BY
  ordering_1 DESC

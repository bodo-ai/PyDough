SELECT
  (
    CAST((
      COALESCE(agg_0, 0) - COALESCE(agg_1, 0)
    ) AS REAL) / COALESCE(agg_2, 0)
  ) * 100 AS GPM
FROM (
  SELECT
    agg_0,
    agg_1,
    agg_2
  FROM (
    SELECT
      SUM(car_cost) AS agg_2,
      SUM(sale_price) AS agg_0
    FROM (
      SELECT
        cost AS car_cost,
        sale_price
      FROM (
        SELECT
          car_id,
          sale_price
        FROM (
          SELECT
            car_id,
            sale_date,
            sale_price
          FROM main.sales
        )
        WHERE
          CAST(STRFTIME('%Y', sale_date) AS INTEGER) = 2023
      )
      LEFT JOIN (
        SELECT
          _id,
          cost
        FROM main.cars
      )
        ON car_id = _id
    )
  )
  LEFT JOIN (
    SELECT
      SUM(cost_7) AS agg_1
    FROM (
      SELECT
        cost AS cost_7
      FROM (
        SELECT
          car_id
        FROM (
          SELECT
            car_id,
            sale_date
          FROM main.sales
        )
        WHERE
          CAST(STRFTIME('%Y', sale_date) AS INTEGER) = 2023
      )
      INNER JOIN (
        SELECT
          _id,
          cost
        FROM main.cars
      )
        ON car_id = _id
    )
  )
    ON TRUE
)

SELECT
  (
    (
      COALESCE(agg_0, 0) - COALESCE(agg_1, 0)
    ) / COALESCE(agg_1, 0)
  ) * 100 AS GPM
FROM (
  SELECT
    agg_0,
    agg_1
  FROM (
    SELECT
      SUM(sale_price) AS agg_0
    FROM (
      SELECT
        sale_price
      FROM (
        SELECT
          sale_date,
          sale_price
        FROM main.sales
      )
      WHERE
        EXTRACT(YEAR FROM sale_date) = 2023
    )
  )
  LEFT JOIN (
    SELECT
      SUM(cost) AS agg_1
    FROM (
      SELECT
        cost
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
          EXTRACT(YEAR FROM sale_date) = 2023
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

SELECT
  payment_week,
  COALESCE(agg_0, 0) AS total_payments,
  COALESCE(agg_1, 0) AS weekend_payments
FROM (
  SELECT
    COUNT() AS agg_0,
    SUM(expr_2) AS agg_1,
    payment_week
  FROM (
    SELECT
      (
        (
          (
            DAY_OF_WEEK(payment_date) + 6
          ) % 7
        )
      ) IN (0, 6) AS expr_2,
      payment_week
    FROM (
      SELECT
        DATE_TRUNC('WEEK', CAST(payment_date AS TIMESTAMP)) AS payment_week,
        payment_date
      FROM (
        SELECT
          payment_date
        FROM (
          SELECT
            payment_date,
            sale_id
          FROM main.payments_received
          WHERE
            (
              1 <= DATEDIFF(CURRENT_TIMESTAMP(), payment_date, WEEK)
            )
            AND (
              DATEDIFF(CURRENT_TIMESTAMP(), payment_date, WEEK) <= 8
            )
        )
        INNER JOIN (
          SELECT
            _id
          FROM (
            SELECT
              _id,
              sale_price
            FROM main.sales
          )
          WHERE
            sale_price > 30000
        )
          ON sale_id = _id
      )
    )
  )
  GROUP BY
    payment_week
)

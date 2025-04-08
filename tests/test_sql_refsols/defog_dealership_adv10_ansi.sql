SELECT
  ROUND(agg_0, 2) AS avg_days_to_payment
FROM (
  SELECT
    AVG(sale_pay_diff) AS agg_0
  FROM (
    SELECT
      DATEDIFF(agg_0, sale_date, DAY) AS sale_pay_diff
    FROM (
      SELECT
        agg_0,
        sale_date
      FROM (
        SELECT
          _id,
          sale_date
        FROM main.sales
      )
      LEFT JOIN (
        SELECT
          MAX(payment_date) AS agg_0,
          sale_id
        FROM (
          SELECT
            payment_date,
            sale_id
          FROM main.payments_received
        )
        GROUP BY
          sale_id
      )
        ON _id = sale_id
    )
  )
)

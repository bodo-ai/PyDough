SELECT
  month,
  total_payments,
  MoM_change
FROM (
  SELECT
    month AS ordering_1,
    total_payments - LAG(total_payments, 1) OVER (ORDER BY month NULLS LAST) AS MoM_change,
    month,
    total_payments
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS total_payments,
      month
    FROM (
      SELECT
        SUM(payment_amount) AS agg_0,
        month
      FROM (
        SELECT
          DATE_TRUNC('MONTH', CAST(payment_date AS TIMESTAMP)) AS month,
          payment_amount
        FROM (
          SELECT
            payment_amount,
            payment_date
          FROM main.payments_received
        )
      )
      GROUP BY
        month
    )
  )
)
ORDER BY
  ordering_1

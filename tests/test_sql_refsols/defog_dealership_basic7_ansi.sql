SELECT
  payment_method,
  total_payments,
  total_amount
FROM (
  SELECT
    ordering_2,
    payment_method,
    total_amount,
    total_payments
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS ordering_2,
      COALESCE(agg_0, 0) AS total_amount,
      COALESCE(agg_1, 0) AS total_payments,
      payment_method
    FROM (
      SELECT
        COUNT() AS agg_1,
        SUM(payment_amount) AS agg_0,
        payment_method
      FROM (
        SELECT
          payment_amount,
          payment_method
        FROM main.payments_received
      )
      GROUP BY
        payment_method
    )
  )
  ORDER BY
    ordering_2 DESC
  LIMIT 3
)
ORDER BY
  ordering_2 DESC

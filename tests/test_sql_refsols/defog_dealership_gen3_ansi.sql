SELECT
  payment_date,
  payment_method,
  total_amount
FROM (
  SELECT
    COALESCE(agg_0, 0) AS total_amount,
    payment_date AS ordering_1,
    payment_method AS ordering_2,
    payment_date,
    payment_method
  FROM (
    SELECT
      SUM(payment_amount) AS agg_0,
      payment_date,
      payment_method
    FROM (
      SELECT
        payment_amount,
        payment_date,
        payment_method
      FROM main.payments_received
      WHERE
        DATEDIFF(CURRENT_TIMESTAMP(), payment_date, WEEK) = 1
    )
    GROUP BY
      payment_date,
      payment_method
  )
)
ORDER BY
  ordering_1 DESC,
  ordering_2

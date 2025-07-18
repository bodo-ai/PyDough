SELECT
  DATE_TRUNC('WEEK', CAST(payments_received.payment_date AS TIMESTAMP)) AS payment_week,
  COUNT(*) AS total_payments,
  COALESCE(
    SUM((
      (
        DAY_OF_WEEK(payments_received.payment_date) + 6
      ) % 7
    ) IN (5, 6)),
    0
  ) AS weekend_payments
FROM main.payments_received AS payments_received
JOIN main.sales AS sales
  ON payments_received.sale_id = sales._id AND sales.sale_price > 30000
WHERE
  DATEDIFF(CURRENT_TIMESTAMP(), CAST(payments_received.payment_date AS DATETIME), WEEK) <= 8
  AND DATEDIFF(CURRENT_TIMESTAMP(), CAST(payments_received.payment_date AS DATETIME), WEEK) >= 1
GROUP BY
  DATE_TRUNC('WEEK', CAST(payments_received.payment_date AS TIMESTAMP))

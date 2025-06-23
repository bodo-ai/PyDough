WITH _t0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM((
      (
        DAY_OF_WEEK(payments_received.payment_date) + 6
      ) % 7
    ) IN (5, 6)) AS agg_1,
    DATE_TRUNC('WEEK', CAST(payments_received.payment_date AS TIMESTAMP)) AS payment_week
  FROM main.payments_received AS payments_received
  JOIN main.sales AS sales
    ON payments_received.sale_id = sales._id AND sales.sale_price > 30000
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), CAST(payments_received.payment_date AS DATETIME), WEEK) <= 8
    AND DATEDIFF(CURRENT_TIMESTAMP(), CAST(payments_received.payment_date AS DATETIME), WEEK) >= 1
  GROUP BY
    DATE_TRUNC('WEEK', CAST(payments_received.payment_date AS TIMESTAMP))
)
SELECT
  payment_week,
  agg_0 AS total_payments,
  COALESCE(agg_1, 0) AS weekend_payments
FROM _t0

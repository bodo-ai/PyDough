WITH _s1 AS (
  SELECT
    sale_id,
    MAX(payment_date) AS max_paymentdate
  FROM main.payments_received
  GROUP BY
    1
)
SELECT
  ROUND(AVG(DATEDIFF(_s1.max_paymentdate, sales.sale_date)), 2) AS avg_days_to_payment
FROM main.sales AS sales
LEFT JOIN _s1 AS _s1
  ON _s1.sale_id = sales._id

WITH _s1 AS (
  SELECT
    MAX(payment_date) AS max_payment_date,
    sale_id
  FROM main.payments_received
  GROUP BY
    sale_id
)
SELECT
  ROUND(AVG(DATEDIFF(_s1.max_payment_date, sales.sale_date)), 2) AS avg_days_to_payment
FROM main.sales AS sales
LEFT JOIN _s1 AS _s1
  ON _s1.sale_id = sales._id

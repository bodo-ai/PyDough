WITH _s1 AS (
  SELECT
    MAX(payment_date) AS max_payment_date,
    sale_id
  FROM main.payments_received
  GROUP BY
    sale_id
), _t0 AS (
  SELECT
    AVG(
      CAST((
        JULIANDAY(DATE(_s1.max_payment_date, 'start of day')) - JULIANDAY(DATE(sales.sale_date, 'start of day'))
      ) AS INTEGER)
    ) AS avg_sale_pay_diff
  FROM main.sales AS sales
  LEFT JOIN _s1 AS _s1
    ON _s1.sale_id = sales._id
)
SELECT
  ROUND(avg_sale_pay_diff, 2) AS avg_days_to_payment
FROM _t0

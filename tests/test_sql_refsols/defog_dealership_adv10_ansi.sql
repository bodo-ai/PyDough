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
      DATEDIFF(CAST(_s1.max_payment_date AS DATETIME), CAST(sales.sale_date AS DATETIME), DAY)
    ) AS avg_sale_pay_diff
  FROM main.sales AS sales
  LEFT JOIN _s1 AS _s1
    ON _s1.sale_id = sales._id
)
SELECT
  ROUND(avg_sale_pay_diff, 2) AS avg_days_to_payment
FROM _t0

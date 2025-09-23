WITH _s1 AS (
  SELECT
    sale_id,
    MAX(payment_date) AS max_payment_date
  FROM main.payments_received
  GROUP BY
    1
)
SELECT
  ROUND(
    AVG(
      DATEDIFF(DAY, CAST(sales.sale_date AS DATETIME), CAST(_s1.max_payment_date AS DATETIME))
    ),
    2
  ) AS avg_days_to_payment
FROM main.sales AS sales
JOIN _s1 AS _s1
  ON _s1.sale_id = sales._id

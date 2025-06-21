WITH _s1 AS (
  SELECT
    MAX(payment_date) AS agg_0,
    sale_id
  FROM main.payments_received
  GROUP BY
    sale_id
), _t0 AS (
  SELECT
    AVG(DATEDIFF(CAST(_s1.agg_0 AS DATETIME), CAST(sales.sale_date AS DATETIME), DAY)) AS agg_0
  FROM main.sales AS sales
  LEFT JOIN _s1 AS _s1
    ON _s1.sale_id = sales._id
)
SELECT
  ROUND(agg_0, 2) AS avg_days_to_payment
FROM _t0

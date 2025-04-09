WITH _t1 AS (
  SELECT
    MAX(payment_date) AS agg_0,
    sale_id
  FROM main.payments_received
  GROUP BY
    sale_id
), _t0_2 AS (
  SELECT
    AVG(DATEDIFF(_t1.agg_0, sales.sale_date, DAY)) AS agg_0
  FROM main.sales AS sales
  LEFT JOIN _t1 AS _t1
    ON _t1.sale_id = sales._id
)
SELECT
  ROUND(agg_0, 2) AS avg_days_to_payment
FROM _t0_2

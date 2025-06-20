WITH _t0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM((
      (
        DAY_OF_WEEK(_s0.payment_date) + 6
      ) % 7
    ) IN (5, 6)) AS agg_1,
    DATE_TRUNC('WEEK', CAST(_s0.payment_date AS TIMESTAMP)) AS payment_week
  FROM main.payments_received AS _s0
  JOIN main.sales AS _s1
    ON _s0.sale_id = _s1._id AND _s1.sale_price > 30000
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), _s0.payment_date, WEEK) <= 8
    AND DATEDIFF(CURRENT_TIMESTAMP(), _s0.payment_date, WEEK) >= 1
  GROUP BY
    DATE_TRUNC('WEEK', CAST(_s0.payment_date AS TIMESTAMP))
)
SELECT
  payment_week,
  agg_0 AS total_payments,
  COALESCE(agg_1, 0) AS weekend_payments
FROM _t0

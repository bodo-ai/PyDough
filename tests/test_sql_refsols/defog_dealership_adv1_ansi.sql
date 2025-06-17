WITH _s0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM((
      (
        DAY_OF_WEEK(payment_date) + 6
      ) % 7
    ) IN (5, 6)) AS agg_1,
    DATE_TRUNC('WEEK', CAST(payment_date AS TIMESTAMP)) AS payment_week,
    sale_id
  FROM main.payments_received
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), payment_date, WEEK) <= 8
    AND DATEDIFF(CURRENT_TIMESTAMP(), payment_date, WEEK) >= 1
  GROUP BY
    DATE_TRUNC('WEEK', CAST(payment_date AS TIMESTAMP)),
    sale_id
), _t0 AS (
  SELECT
    SUM(_s0.agg_0) AS agg_0,
    SUM(_s0.agg_1) AS agg_1,
    _s0.payment_week
  FROM _s0 AS _s0
  JOIN main.sales AS sales
    ON _s0.sale_id = sales._id AND sales.sale_price > 30000
  GROUP BY
    _s0.payment_week
)
SELECT
  payment_week,
  agg_0 AS total_payments,
  COALESCE(agg_1, 0) AS weekend_payments
FROM _t0

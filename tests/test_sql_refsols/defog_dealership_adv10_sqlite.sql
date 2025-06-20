WITH _s3 AS (
  SELECT
    MAX(payment_date) AS agg_0,
    sale_id
  FROM main.payments_received
  GROUP BY
    sale_id
), _t0 AS (
  SELECT
    AVG(
      CAST((
        JULIANDAY(DATE(_s3.agg_0, 'start of day')) - JULIANDAY(DATE(_s0.sale_date, 'start of day'))
      ) AS INTEGER)
    ) AS agg_0
  FROM main.sales AS _s0
  LEFT JOIN _s3 AS _s3
    ON _s0._id = _s3.sale_id
)
SELECT
  ROUND(agg_0, 2) AS avg_days_to_payment
FROM _t0

WITH _s1 AS (
  SELECT
    DATE_TRUNC('MONTH', CAST(payment_date AS TIMESTAMP)) AS start_month,
    SUM(payment_amount) AS sum_payment_amount
  FROM main.payments_received
  GROUP BY
    1
), _t0 AS (
  SELECT
    _s1.start_month + INTERVAL (
      months.n
    ) MONTH AS dt,
    SUM(CASE WHEN months.n > 0 THEN 0 ELSE COALESCE(_s1.sum_payment_amount, 0) END) AS sum_payment
  FROM (VALUES
    (0),
    (1),
    (2),
    (3),
    (4),
    (5),
    (6),
    (7),
    (8),
    (9),
    (10),
    (11)) AS months(n)
  JOIN _s1 AS _s1
    ON (
      _s1.start_month + INTERVAL (months.n) MONTH
    ) <= DATE_TRUNC('MONTH', CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)) + INTERVAL '1' HOUR
  GROUP BY
    1
)
SELECT
  dt,
  sum_payment AS total_payments,
  sum_payment - LAG(sum_payment, 1) OVER (ORDER BY dt) AS MoM_change
FROM _t0
ORDER BY
  1 NULLS FIRST

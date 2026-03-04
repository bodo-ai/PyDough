WITH _s1 AS (
  SELECT
    DATE_TRUNC('MONTH', CAST(payment_date AS TIMESTAMP)) AS start_month,
    SUM(payment_amount) AS sum_payment_amount
  FROM main.payments_received
  GROUP BY
    1
), _t0 AS (
  SELECT
    DATE_ADD('N', CAST(_s1.start_month AS BIGINT), 'month') AS dt,
    SUM(IF(months.n > 0, 0, COALESCE(_s1.sum_payment_amount, 0))) AS sum_payment
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
    ON DATE_ADD('N', CAST(_s1.start_month AS BIGINT), 'month') <= DATE_ADD('HOUR', 1, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP))
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

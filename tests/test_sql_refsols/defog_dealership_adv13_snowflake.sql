WITH months AS (
  SELECT
    SEQ4() AS n
  FROM TABLE(GENERATOR(ROWCOUNT => 12))
), _s1 AS (
  SELECT
    DATE_TRUNC('MONTH', CAST(payment_date AS TIMESTAMP)) AS start_month,
    SUM(payment_amount) AS sum_payment_amount
  FROM dealership.payments_received
  GROUP BY
    1
), _t0 AS (
  SELECT
    DATEADD(MONTH, months.n, _s1.start_month) AS dt,
    SUM(IFF(months.n > 0, 0, COALESCE(_s1.sum_payment_amount, 0))) AS sum_payment
  FROM months AS months
  JOIN _s1 AS _s1
    ON DATEADD(
      HOUR,
      1,
      DATE_TRUNC('MONTH', CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ))
    ) >= DATEADD(MONTH, months.n, _s1.start_month)
  GROUP BY
    1
)
SELECT
  dt,
  sum_payment AS total_payments,
  sum_payment - LAG(sum_payment, 1) OVER (ORDER BY dt) AS MoM_change
FROM _t0

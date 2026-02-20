WITH _s1 AS (
  SELECT
    STR_TO_DATE(
      CONCAT(YEAR(CAST(payment_date AS DATETIME)), ' ', MONTH(CAST(payment_date AS DATETIME)), ' 1'),
      '%Y %c %e'
    ) AS start_month,
    SUM(payment_amount) AS sum_payment_amount
  FROM dealership.payments_received
  GROUP BY
    1
), _t0 AS (
  SELECT
    _s1.start_month + INTERVAL months.n MONTH AS dt,
    SUM(CASE WHEN months.n > 0 THEN 0 ELSE COALESCE(_s1.sum_payment_amount, 0) END) AS sum_payment
  FROM (VALUES
    ROW(0),
    ROW(1),
    ROW(2),
    ROW(3),
    ROW(4),
    ROW(5),
    ROW(6),
    ROW(7),
    ROW(8),
    ROW(9),
    ROW(10),
    ROW(11)) AS months(n)
  JOIN _s1 AS _s1
    ON (
      _s1.start_month + INTERVAL months.n MONTH
    ) <= DATE_ADD(
      CAST(STR_TO_DATE(
        CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
        '%Y %c %e'
      ) AS DATETIME),
      INTERVAL '1' HOUR
    )
  GROUP BY
    1
)
SELECT
  dt,
  sum_payment AS total_payments,
  sum_payment - LAG(sum_payment, 1) OVER (ORDER BY CASE WHEN dt IS NULL THEN 1 ELSE 0 END, dt) AS MoM_change
FROM _t0

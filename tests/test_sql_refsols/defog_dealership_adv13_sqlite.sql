WITH _s1 AS (
  SELECT
    DATE(payment_date, 'start of month') AS start_month,
    SUM(payment_amount) AS sum_payment_amount
  FROM main.payments_received
  GROUP BY
    1
), _t0 AS (
  SELECT
    DATETIME(_s1.start_month, months.column1 || ' months') AS dt,
    SUM(IIF(months.column1 > 0, 0, COALESCE(_s1.sum_payment_amount, 0))) AS sum_payment
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
    (11)) AS months
  JOIN _s1 AS _s1
    ON DATETIME(DATE('now', 'start of month'), '1 hour') >= DATETIME(_s1.start_month, months.column1 || ' months')
  GROUP BY
    1
)
SELECT
  dt,
  sum_payment AS total_payments,
  sum_payment - LAG(sum_payment, 1) OVER (ORDER BY dt) AS MoM_change
FROM _t0

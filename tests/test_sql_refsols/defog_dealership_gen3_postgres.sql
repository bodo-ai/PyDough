SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM main.payments_received
WHERE
  CAST(CAST(EXTRACT(EPOCH FROM (
    CURRENT_TIMESTAMP - payment_date
  )) AS DOUBLE PRECISION) / 604800 AS BIGINT) = 1
GROUP BY
  1,
  2
ORDER BY
  1 DESC NULLS LAST,
  2 NULLS FIRST

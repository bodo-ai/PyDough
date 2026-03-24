SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM postgres.main.payments_received
WHERE
  DATE_DIFF(
    'WEEK',
    CAST(DATE_TRUNC(
      'DAY',
      DATE_ADD(
        'DAY',
        (
          (
            (
              DAY_OF_WEEK(CAST(payment_date AS TIMESTAMP)) % 7
            ) + 0
          ) % 7
        ) * -1,
        CAST(payment_date AS TIMESTAMP)
      )
    ) AS TIMESTAMP),
    CAST(DATE_TRUNC(
      'DAY',
      DATE_ADD(
        'DAY',
        (
          (
            (
              DAY_OF_WEEK(CURRENT_TIMESTAMP) % 7
            ) + 0
          ) % 7
        ) * -1,
        CURRENT_TIMESTAMP
      )
    ) AS TIMESTAMP)
  ) = 1
GROUP BY
  1,
  2
ORDER BY
  1 DESC,
  2 NULLS FIRST

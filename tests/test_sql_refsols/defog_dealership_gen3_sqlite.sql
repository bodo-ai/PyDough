SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM main.payments_received
WHERE
  CAST(CAST(CAST((
    JULIANDAY(
      DATE(
        DATETIME('now'),
        '-' || CAST((
          CAST(STRFTIME('%w', DATETIME(DATETIME('now'))) AS INTEGER) + 6
        ) % 7 AS TEXT) || ' days',
        'start of day'
      )
    ) - JULIANDAY(
      DATE(
        payment_date,
        '-' || CAST((
          CAST(STRFTIME('%w', DATETIME(payment_date)) AS INTEGER) + 6
        ) % 7 AS TEXT) || ' days',
        'start of day'
      )
    )
  ) AS INTEGER) AS REAL) / 7 AS INTEGER) = 1
GROUP BY
  payment_date,
  payment_method
ORDER BY
  payment_date DESC,
  payment_method

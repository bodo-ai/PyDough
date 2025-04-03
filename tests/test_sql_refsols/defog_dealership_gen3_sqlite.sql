SELECT
  payment_date,
  payment_method,
  total_amount
FROM (
  SELECT
    COALESCE(agg_0, 0) AS total_amount,
    payment_date AS ordering_1,
    payment_method AS ordering_2,
    payment_date,
    payment_method
  FROM (
    SELECT
      SUM(payment_amount) AS agg_0,
      payment_date,
      payment_method
    FROM (
      SELECT
        payment_amount,
        payment_date,
        payment_method
      FROM main.payments_received
      WHERE
        CAST(CAST(CAST((JULIANDAY(DATE(
          DATE(
            DATETIME('now'),
            '-' || CAST((
              CAST(STRFTIME('%w', DATETIME(DATETIME('now'))) AS INTEGER) + 6
            ) % 7 AS TEXT) || ' days',
            'start of day'
          ),
          'start of day'
        )) - JULIANDAY(DATE(
          DATE(
            payment_date,
            '-' || CAST((
              CAST(STRFTIME('%w', DATETIME(payment_date)) AS INTEGER) + 6
            ) % 7 AS TEXT) || ' days',
            'start of day'
          ),
          'start of day'
        ))) AS INTEGER) AS REAL) / 7 AS INTEGER) = 1
    )
    GROUP BY
      payment_date,
      payment_method
  )
)
ORDER BY
  ordering_1 DESC,
  ordering_2

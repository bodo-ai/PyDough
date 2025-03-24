SELECT
  month,
  avg_price
FROM (
  SELECT
    AVG(price) AS avg_price,
    month AS ordering_1,
    month
  FROM (
    SELECT
      DATE(date_time, 'start of month') AS month,
      price
    FROM (
      SELECT
        date_time,
        price
      FROM (
        SELECT
          sbTxDateTime AS date_time,
          sbTxPrice AS price,
          sbTxStatus AS status
        FROM main.sbTransaction
      )
      WHERE
        (
          status = 'success'
        )
        AND (
          CAST((JULIANDAY(DATE(DATETIME('2023-03-31'), 'start of day')) - JULIANDAY(DATE(date_time, 'start of day'))) AS INTEGER) >= 0
        )
        AND (
          CAST((JULIANDAY(DATE(date_time, 'start of day')) - JULIANDAY(DATE(DATETIME('2023-01-01'), 'start of day'))) AS INTEGER) >= 0
        )
    )
  )
  GROUP BY
    month
)
ORDER BY
  ordering_1

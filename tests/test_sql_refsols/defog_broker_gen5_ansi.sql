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
      DATE_TRUNC('MONTH', DATETIME(date_time)) AS month,
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
          DATEDIFF('2023-03-31', date_time, DAY) >= 0
        )
        AND (
          DATEDIFF(date_time, '2023-01-01', DAY) >= 0
        )
    )
  )
  GROUP BY
    month
)
ORDER BY
  ordering_1

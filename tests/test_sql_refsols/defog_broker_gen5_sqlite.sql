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
          (
            '2023-01-01' <= date_time
          ) AND (
            date_time <= '2023-03-31'
          )
        )
    )
  )
  GROUP BY
    month
)
ORDER BY
  ordering_1

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
      DATE_TRUNC('MONTH', CAST(date_time AS TIMESTAMP)) AS month,
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
          DATEDIFF(CAST('2023-03-31' AS TIMESTAMP), date_time, DAY) >= 0
        )
        AND (
          DATEDIFF(date_time, CAST('2023-01-01' AS TIMESTAMP), DAY) >= 0
        )
    )
  )
  GROUP BY
    month
)
ORDER BY
  ordering_1

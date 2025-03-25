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
      ) AS _t3
      WHERE
        (
          status = 'success'
        )
        AND (
          (
            CAST('2023-01-01' AS DATE) <= date_time
          )
          AND (
            date_time <= CAST('2023-03-31' AS DATE)
          )
        )
    ) AS _t2
  ) AS _t1
  GROUP BY
    month
) AS _t0
ORDER BY
  ordering_1

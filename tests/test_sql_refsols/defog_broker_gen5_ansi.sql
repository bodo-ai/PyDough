WITH _t0 AS (
  SELECT
    AVG(sbtxprice) AS avg_price,
    DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP)) AS ordering_1,
    DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP)) AS month
  FROM main.sbtransaction
  WHERE
    sbtxdatetime <= CAST('2023-03-31' AS DATE)
    AND sbtxdatetime >= CAST('2023-01-01' AS DATE)
    AND sbtxstatus = 'success'
  GROUP BY
    DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP))
)
SELECT
  month,
  avg_price
FROM _t0
ORDER BY
  ordering_1

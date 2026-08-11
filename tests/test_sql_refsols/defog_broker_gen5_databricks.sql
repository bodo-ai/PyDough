SELECT
  TRUNC(CAST(sbtxdatetime AS TIMESTAMP), 'MONTH') AS month,
  AVG(sbtxprice) AS avg_price
FROM defog.broker.sbtransaction
WHERE
  EXTRACT(MONTH FROM CAST(sbtxdatetime AS TIMESTAMP)) IN (1, 2, 3)
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS TIMESTAMP)) = 2023
  AND sbtxstatus = 'success'
GROUP BY
  1
ORDER BY
  1

SELECT
  DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP)) AS month,
  AVG(sbtxprice) AS avg_price
FROM main.sbtransaction
WHERE
  EXTRACT(QUARTER FROM CAST(sbtxdatetime AS DATETIME)) = 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)) = 2023
  AND sbtxstatus = 'success'
GROUP BY
  1
ORDER BY
  month

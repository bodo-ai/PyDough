SELECT
  STR_TO_DATE(
    CONCAT(YEAR(CAST(sbtxdatetime AS DATETIME)), ' ', MONTH(CAST(sbtxdatetime AS DATETIME)), ' 1'),
    '%Y %c %e'
  ) AS month,
  AVG(sbtxprice) AS avg_price
FROM main.sbTransaction
WHERE
  EXTRACT(QUARTER FROM CAST(sbtxdatetime AS DATETIME)) = 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)) = 2023
  AND sbtxstatus = 'success'
GROUP BY
  1
ORDER BY
  1

SELECT
  STR_TO_DATE(
    CONCAT(YEAR(CAST(sbtxdatetime AS DATETIME)), ' ', MONTH(CAST(sbtxdatetime AS DATETIME)), ' 1'),
    '%Y %c %e'
  ) AS month,
  AVG(sbtxprice) AS avg_price
FROM main.sbTransaction
WHERE
  QUARTER(sbtxdatetime) = 1 AND YEAR(sbtxdatetime) = 2023 AND sbtxstatus = 'success'
GROUP BY
  STR_TO_DATE(
    CONCAT(YEAR(CAST(sbtxdatetime AS DATETIME)), ' ', MONTH(CAST(sbtxdatetime AS DATETIME)), ' 1'),
    '%Y %c %e'
  )
ORDER BY
  month

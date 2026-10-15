SELECT
  STR_TO_DATE(
    CONCAT(YEAR(CAST(sbTxDateTime AS DATETIME)), ' ', MONTH(CAST(sbTxDateTime AS DATETIME)), ' 1'),
    '%Y %c %e'
  ) AS month,
  AVG(sbTxPrice) AS avg_price
FROM broker.sbTransaction
WHERE
  EXTRACT(MONTH FROM CAST(sbTxDateTime AS DATETIME)) IN (1, 2, 3)
  AND EXTRACT(YEAR FROM CAST(sbTxDateTime AS DATETIME)) = 2023
  AND sbTxStatus = 'success'
GROUP BY
  1
ORDER BY
  1

SELECT
  month COLLATE utf8mb4_bin AS month,
  COUNT(DISTINCT diag_id) AS PMPD,
  COUNT(*) AS PMTC
FROM main.treatments
WHERE
  DATE_ADD(
    STR_TO_DATE(
      CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
      '%Y %c %e'
    ),
    INTERVAL '-12' MONTH
  ) <= STR_TO_DATE(
    CONCAT(YEAR(CAST(start_dt AS DATETIME)), ' ', MONTH(CAST(start_dt AS DATETIME)), ' 1'),
    '%Y %c %e'
  )
  AND STR_TO_DATE(
    CONCAT(YEAR(CAST(start_dt AS DATETIME)), ' ', MONTH(CAST(start_dt AS DATETIME)), ' 1'),
    '%Y %c %e'
  ) < STR_TO_DATE(
    CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
    '%Y %c %e'
  )
GROUP BY
  1
ORDER BY
  1 DESC

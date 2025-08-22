WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows,
    COUNT(DISTINCT diag_id) AS ndistinct_diag_id,
    STR_TO_DATE(
      CONCAT(YEAR(CAST(start_dt AS DATETIME)), ' ', MONTH(CAST(start_dt AS DATETIME)), ' 1'),
      '%Y %c %e'
    ) AS start_month
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
    3
)
SELECT
  CONCAT_WS('-', EXTRACT(YEAR FROM start_month), LPAD(EXTRACT(MONTH FROM start_month), 2, '0')) AS start_month,
  ndistinct_diag_id AS PMPD,
  n_rows AS PMTC
FROM _t0
ORDER BY
  1 DESC

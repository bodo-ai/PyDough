WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows,
    COUNT(DISTINCT diag_id) AS ndistinct_diag_id,
    DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP)) AS start_month
  FROM main.treatments
  WHERE
    DATEADD(MONTH, -12, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())) <= DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP))
    AND DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP)) < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
  GROUP BY
    3
)
SELECT
  CONCAT_WS(
    '-',
    YEAR(CAST(start_month AS TIMESTAMP)),
    LPAD(MONTH(CAST(start_month AS TIMESTAMP)), 2, '0')
  ) AS start_month,
  ndistinct_diag_id AS PMPD,
  n_rows AS PMTC
FROM _t0
ORDER BY
  1 DESC NULLS LAST

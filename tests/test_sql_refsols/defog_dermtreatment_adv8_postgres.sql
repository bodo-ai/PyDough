WITH _t0 AS (
  SELECT
    DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP)) AS start_month,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT diag_id) AS ndistinct_diag_id
  FROM main.treatments
  WHERE
    DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP)) < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP)
    AND DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP)) >= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP) - INTERVAL '12 MONTH'
  GROUP BY
    1
)
SELECT
  CONCAT_WS(
    '-',
    EXTRACT(YEAR FROM CAST(start_month AS TIMESTAMP)),
    LPAD(CAST(EXTRACT(MONTH FROM CAST(start_month AS TIMESTAMP)) AS TEXT), 2, '0')
  ) AS start_month,
  ndistinct_diag_id AS PMPD,
  n_rows AS PMTC
FROM _t0
ORDER BY
  1 DESC NULLS LAST

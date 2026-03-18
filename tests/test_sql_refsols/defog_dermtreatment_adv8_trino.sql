WITH _t0 AS (
  SELECT
    DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP)) AS start_month,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT diag_id) AS ndistinct_diag_id
  FROM postgres.treatments
  WHERE
    DATE_ADD('MONTH', -12, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP)) <= DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP))
    AND DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP)) < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP)
  GROUP BY
    1
)
SELECT
  CONCAT_WS(
    '-'[0],
    CAST(YEAR(CAST(start_month AS TIMESTAMP))[0] AS VARCHAR),
    CAST(LPAD(MONTH(CAST(start_month AS TIMESTAMP)), 2, '0')[1] AS VARCHAR)
  ) AS start_month,
  ndistinct_diag_id AS PMPD,
  n_rows AS PMTC
FROM _t0
ORDER BY
  1 DESC

WITH _t0 AS (
  SELECT
    DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP)) AS start_month,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT diag_id) AS ndistinct_diag_id
  FROM dermtreatment.treatments
  WHERE
    DATEADD(
      MONTH,
      -12,
      DATE_TRUNC('MONTH', CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ))
    ) <= DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP))
    AND DATE_TRUNC('MONTH', CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)) > DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP))
  GROUP BY
    1
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

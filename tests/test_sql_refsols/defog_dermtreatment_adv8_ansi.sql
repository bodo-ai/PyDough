WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows,
    COUNT(DISTINCT diag_id) AS ndistinct_diag_id,
    DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP)) AS start_month
  FROM main.treatments
  WHERE
    DATE_ADD(DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()), -12, 'MONTH') <= DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP))
    AND DATE_TRUNC('MONTH', CAST(start_dt AS TIMESTAMP)) < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
  GROUP BY
    3
)
SELECT
  CONCAT_WS(
    '-',
    EXTRACT(YEAR FROM start_month),
    CASE
      WHEN LENGTH(EXTRACT(MONTH FROM start_month)) >= 2
      THEN SUBSTRING(EXTRACT(MONTH FROM start_month), 1, 2)
      ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM start_month)), -2)
    END
  ) AS start_month,
  ndistinct_diag_id AS PMPD,
  n_rows AS PMTC
FROM _t0
ORDER BY
  1 DESC

WITH _t0 AS (
  SELECT
    TRUNC(CAST(start_dt AS TIMESTAMP), 'MONTH') AS start_month,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT diag_id) AS ndistinct_diag_id
  FROM main.treatments
  WHERE
    ADD_MONTHS(TRUNC(CURRENT_TIMESTAMP(), 'MONTH'), -12) <= TRUNC(CAST(start_dt AS TIMESTAMP), 'MONTH')
    AND TRUNC(CAST(start_dt AS TIMESTAMP), 'MONTH') < TRUNC(CURRENT_TIMESTAMP(), 'MONTH')
  GROUP BY
    1
)
SELECT
  CONCAT_WS('-', EXTRACT(YEAR FROM start_month), LPAD(EXTRACT(MONTH FROM start_month), 2, '0')) AS start_month,
  ndistinct_diag_id AS PMPD,
  n_rows AS PMTC
FROM _t0
ORDER BY
  1 DESC

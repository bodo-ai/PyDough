WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows,
    COUNT(DISTINCT diag_id) AS ndistinct_diag_id,
    DATE(start_dt, 'start of month') AS start_month
  FROM main.treatments
  WHERE
    DATE('now', 'start of month', '-12 month') <= DATE(start_dt, 'start of month')
    AND DATE('now', 'start of month') > DATE(start_dt, 'start of month')
  GROUP BY
    3
)
SELECT
  CONCAT_WS(
    '-',
    CAST(STRFTIME('%Y', start_month) AS INTEGER),
    CASE
      WHEN LENGTH(CAST(STRFTIME('%m', start_month) AS INTEGER)) >= 2
      THEN SUBSTRING(CAST(STRFTIME('%m', start_month) AS INTEGER), 1, 2)
      ELSE SUBSTRING('00' || CAST(STRFTIME('%m', start_month) AS INTEGER), -2)
    END
  ) AS start_month,
  ndistinct_diag_id AS PMPD,
  n_rows AS PMTC
FROM _t0
ORDER BY
  1 DESC

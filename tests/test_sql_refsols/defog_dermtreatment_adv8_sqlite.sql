SELECT
  CONCAT_WS(
    '-',
    CAST(STRFTIME('%Y', start_dt) AS INTEGER),
    CASE
      WHEN LENGTH(CAST(STRFTIME('%m', start_dt) AS INTEGER)) >= 2
      THEN SUBSTRING(CAST(STRFTIME('%m', start_dt) AS INTEGER), 1, 2)
      ELSE SUBSTRING('00' || CAST(STRFTIME('%m', start_dt) AS INTEGER), (
        2 * -1
      ))
    END
  ) AS month,
  COUNT(DISTINCT diag_id) AS PMPD,
  COUNT(*) AS PMTC
FROM main.treatments
WHERE
  DATE('now', 'start of month', '-12 month') <= DATE(start_dt, 'start of month')
  AND DATE('now', 'start of month') > DATE(start_dt, 'start of month')
GROUP BY
  1
ORDER BY
  1 DESC

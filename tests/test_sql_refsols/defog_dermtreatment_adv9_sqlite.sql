WITH _s2 AS (
  SELECT
    COUNT(DISTINCT patient_id) AS ndistinct_patient_id,
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
    ) AS treatment_month
  FROM main.treatments
  WHERE
    start_dt < DATE('now', 'start of month')
    AND start_dt >= DATE('now', 'start of month', '-3 month')
  GROUP BY
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
    )
), _s3 AS (
  SELECT
    COUNT(DISTINCT treatments.patient_id) AS ndistinct_patient_id,
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', treatments.start_dt) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', treatments.start_dt) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', treatments.start_dt) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', treatments.start_dt) AS INTEGER), (
          2 * -1
        ))
      END
    ) AS treatment_month
  FROM main.treatments AS treatments
  JOIN main.drugs AS drugs
    ON drugs.drug_id = treatments.drug_id AND drugs.drug_type = 'biologic'
  WHERE
    treatments.start_dt < DATE('now', 'start of month')
    AND treatments.start_dt >= DATE('now', 'start of month', '-3 month')
  GROUP BY
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', treatments.start_dt) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', treatments.start_dt) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', treatments.start_dt) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', treatments.start_dt) AS INTEGER), (
          2 * -1
        ))
      END
    )
)
SELECT
  _s2.treatment_month AS month,
  _s2.ndistinct_patient_id AS patient_count,
  COALESCE(_s3.ndistinct_patient_id, 0) AS biologic_treatment_count
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.treatment_month = _s3.treatment_month
ORDER BY
  _s2.treatment_month DESC

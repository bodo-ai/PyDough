WITH _s2 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(start_dt AS TIMESTAMP)),
      LPAD(CAST(EXTRACT(MONTH FROM CAST(start_dt AS TIMESTAMP)) AS TEXT), 2, '0')
    ) AS treatment_month,
    COUNT(DISTINCT patient_id) AS ndistinct_patientid
  FROM main.treatments
  WHERE
    start_dt < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP)
    AND start_dt >= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP) - INTERVAL '3 MONTH'
  GROUP BY
    1
), _s3 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(treatments.start_dt AS TIMESTAMP)),
      LPAD(CAST(EXTRACT(MONTH FROM CAST(treatments.start_dt AS TIMESTAMP)) AS TEXT), 2, '0')
    ) AS treatment_month,
    COUNT(DISTINCT treatments.patient_id) AS ndistinct_patientid
  FROM main.treatments AS treatments
  JOIN main.drugs AS drugs
    ON drugs.drug_id = treatments.drug_id AND drugs.drug_type = 'biologic'
  WHERE
    treatments.start_dt < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP)
    AND treatments.start_dt >= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP) - INTERVAL '3 MONTH'
  GROUP BY
    1
)
SELECT
  _s2.treatment_month AS month,
  _s2.ndistinct_patientid AS patient_count,
  COALESCE(_s3.ndistinct_patientid, 0) AS biologic_treatment_count
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.treatment_month = _s3.treatment_month
ORDER BY
  1 DESC NULLS LAST

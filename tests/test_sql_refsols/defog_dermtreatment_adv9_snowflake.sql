WITH _s2 AS (
  SELECT
    CONCAT_WS(
      '-',
      YEAR(CAST(start_dt AS TIMESTAMP)),
      LPAD(MONTH(CAST(start_dt AS TIMESTAMP)), 2, '0')
    ) AS treatment_month,
    COUNT(DISTINCT patient_id) AS ndistinct_patient_id
  FROM main.treatments
  WHERE
    start_dt < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND start_dt >= DATEADD(MONTH, -3, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))
  GROUP BY
    1
), _s3 AS (
  SELECT
    CONCAT_WS(
      '-',
      YEAR(CAST(treatments.start_dt AS TIMESTAMP)),
      LPAD(MONTH(CAST(treatments.start_dt AS TIMESTAMP)), 2, '0')
    ) AS treatment_month,
    COUNT(DISTINCT treatments.patient_id) AS ndistinct_patient_id
  FROM main.treatments AS treatments
  JOIN main.drugs AS drugs
    ON drugs.drug_id = treatments.drug_id AND drugs.drug_type = 'biologic'
  WHERE
    treatments.start_dt < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND treatments.start_dt >= DATEADD(MONTH, -3, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))
  GROUP BY
    1
)
SELECT
  _s2.treatment_month AS month,
  _s2.ndistinct_patient_id AS patient_count,
  _s3.ndistinct_patient_id AS biologic_treatment_count
FROM _s2 AS _s2
JOIN _s3 AS _s3
  ON _s2.treatment_month = _s3.treatment_month
ORDER BY
  1 DESC NULLS LAST

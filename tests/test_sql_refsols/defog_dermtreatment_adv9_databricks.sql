WITH _s2 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(start_dt AS TIMESTAMP)),
      LPAD(EXTRACT(MONTH FROM CAST(start_dt AS TIMESTAMP)), 2, '0')
    ) AS treatment_month,
    COUNT(DISTINCT patient_id) AS ndistinct_patient_id
  FROM defog.dermtreatment.treatments
  WHERE
    start_dt < TRUNC(CURRENT_TIMESTAMP(), 'MONTH')
    AND start_dt >= ADD_MONTHS(TRUNC(CURRENT_TIMESTAMP(), 'MONTH'), -3)
  GROUP BY
    1
), _s3 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(treatments.start_dt AS TIMESTAMP)),
      LPAD(EXTRACT(MONTH FROM CAST(treatments.start_dt AS TIMESTAMP)), 2, '0')
    ) AS treatment_month,
    COUNT(DISTINCT treatments.patient_id) AS ndistinct_patient_id
  FROM defog.dermtreatment.treatments AS treatments
  JOIN defog.dermtreatment.drugs AS drugs
    ON drugs.drug_id = treatments.drug_id AND drugs.drug_type = 'biologic'
  WHERE
    treatments.start_dt < TRUNC(CURRENT_TIMESTAMP(), 'MONTH')
    AND treatments.start_dt >= ADD_MONTHS(TRUNC(CURRENT_TIMESTAMP(), 'MONTH'), -3)
  GROUP BY
    1
)
SELECT
  _s2.treatment_month AS month,
  _s2.ndistinct_patient_id AS patient_count,
  COALESCE(_s3.ndistinct_patient_id, 0) AS biologic_treatment_count
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.treatment_month = _s3.treatment_month
ORDER BY
  1 DESC

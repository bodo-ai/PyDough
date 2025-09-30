WITH _s2 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(start_dt AS DATETIME)),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM CAST(start_dt AS DATETIME))) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM CAST(start_dt AS DATETIME)), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM CAST(start_dt AS DATETIME))), (
          2 * -1
        ))
      END
    ) AS treatment_month,
    COUNT(DISTINCT patient_id) AS ndistinct_patient_id
  FROM main.treatments
  WHERE
    start_dt < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND start_dt >= DATE_SUB(DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()), 3, MONTH)
  GROUP BY
    1
), _s3 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(treatments.start_dt AS DATETIME)),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM CAST(treatments.start_dt AS DATETIME))) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM CAST(treatments.start_dt AS DATETIME)), 1, 2)
        ELSE SUBSTRING(
          CONCAT('00', EXTRACT(MONTH FROM CAST(treatments.start_dt AS DATETIME))),
          (
            2 * -1
          )
        )
      END
    ) AS treatment_month,
    COUNT(DISTINCT treatments.patient_id) AS ndistinct_patient_id
  FROM main.treatments AS treatments
  JOIN main.drugs AS drugs
    ON drugs.drug_id = treatments.drug_id AND drugs.drug_type = 'biologic'
  WHERE
    treatments.start_dt < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    AND treatments.start_dt >= DATE_SUB(DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()), 3, MONTH)
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
  1 DESC

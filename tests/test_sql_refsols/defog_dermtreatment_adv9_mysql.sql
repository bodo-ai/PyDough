WITH _s2 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(start_dt AS DATETIME)),
      LPAD(EXTRACT(MONTH FROM CAST(start_dt AS DATETIME)), 2, '0')
    ) AS treatment_month,
    COUNT(DISTINCT patient_id) AS ndistinct_patient_id
  FROM treatments
  WHERE
    start_dt < STR_TO_DATE(
      CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
      '%Y %c %e'
    )
    AND start_dt >= DATE_SUB(
      STR_TO_DATE(
        CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
        '%Y %c %e'
      ),
      INTERVAL '3' MONTH
    )
  GROUP BY
    1
), _s3 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(treatments.start_dt AS DATETIME)),
      LPAD(EXTRACT(MONTH FROM CAST(treatments.start_dt AS DATETIME)), 2, '0')
    ) AS treatment_month,
    COUNT(DISTINCT treatments.patient_id) AS ndistinct_patient_id
  FROM treatments AS treatments
  JOIN drugs AS drugs
    ON drugs.drug_id = treatments.drug_id AND drugs.drug_type = 'biologic'
  WHERE
    treatments.start_dt < STR_TO_DATE(
      CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
      '%Y %c %e'
    )
    AND treatments.start_dt >= DATE_SUB(
      STR_TO_DATE(
        CONCAT(YEAR(CURRENT_TIMESTAMP()), ' ', MONTH(CURRENT_TIMESTAMP()), ' 1'),
        '%Y %c %e'
      ),
      INTERVAL '3' MONTH
    )
  GROUP BY
    1
)
SELECT
  _s2.treatment_month COLLATE utf8mb4_bin AS month,
  _s2.ndistinct_patient_id AS patient_count,
  COALESCE(_s3.ndistinct_patient_id, 0) AS biologic_treatment_count
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.treatment_month = _s3.treatment_month
ORDER BY
  1 DESC

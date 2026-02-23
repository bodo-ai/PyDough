WITH "_u_0" AS (
  SELECT
    TREATMENTS.patient_id AS "_u_1"
  FROM MAIN.TREATMENTS TREATMENTS
  JOIN MAIN.DIAGNOSES DIAGNOSES
    ON DIAGNOSES.diag_id = TREATMENTS.diag_id
    AND LOWER(DIAGNOSES.diag_name) = 'psoriasis vulgaris'
  JOIN MAIN.DRUGS DRUGS
    ON DRUGS.drug_id = TREATMENTS.drug_id AND LOWER(DRUGS.drug_type) = 'biologic'
  GROUP BY
    TREATMENTS.patient_id
)
SELECT
  COUNT(*) AS patient_count
FROM MAIN.PATIENTS PATIENTS
LEFT JOIN "_u_0" "_u_0"
  ON PATIENTS.patient_id = "_u_0"."_u_1"
WHERE
  NOT "_u_0"."_u_1" IS NULL

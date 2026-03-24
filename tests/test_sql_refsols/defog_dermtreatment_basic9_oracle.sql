WITH "_u_0" AS (
  SELECT
    patient_id AS "_u_1"
  FROM MAIN.TREATMENTS
  GROUP BY
    patient_id
)
SELECT
  PATIENTS.patient_id,
  PATIENTS.first_name,
  PATIENTS.last_name
FROM MAIN.PATIENTS PATIENTS
LEFT JOIN "_u_0" "_u_0"
  ON PATIENTS.patient_id = "_u_0"."_u_1"
WHERE
  "_u_0"."_u_1" IS NULL

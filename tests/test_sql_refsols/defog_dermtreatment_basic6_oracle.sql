WITH "_u_0" AS (
  SELECT
    TREATMENTS.patient_id AS "_u_1"
  FROM MAIN.TREATMENTS TREATMENTS
  JOIN MAIN.OUTCOMES OUTCOMES
    ON OUTCOMES.treatment_id = TREATMENTS.treatment_id
  GROUP BY
    TREATMENTS.patient_id
)
SELECT
  PATIENTS.patient_id,
  PATIENTS.first_name,
  PATIENTS.last_name
FROM MAIN.PATIENTS PATIENTS
LEFT JOIN "_u_0" "_u_0"
  ON PATIENTS.patient_id = "_u_0"."_u_1"
WHERE
  NOT "_u_0"."_u_1" IS NULL

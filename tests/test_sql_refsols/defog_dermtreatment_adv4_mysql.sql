SELECT
  COUNT(*) AS patient_count
FROM patients
WHERE
  EXISTS(
    SELECT
      1 AS `1`
    FROM treatments AS treatments
    JOIN diagnoses AS diagnoses
      ON LOWER(diagnoses.diag_name) = 'psoriasis vulgaris'
      AND diagnoses.diag_id = treatments.diag_id
    JOIN drugs AS drugs
      ON LOWER(drugs.drug_type) = 'biologic' AND drugs.drug_id = treatments.drug_id
    WHERE
      patients.patient_id = treatments.patient_id
  )

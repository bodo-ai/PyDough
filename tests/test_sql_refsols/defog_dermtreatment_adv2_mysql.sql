SELECT
  AVG(patients.weight_kg) AS avg_weight
FROM treatments AS treatments
JOIN drugs AS drugs
  ON LOWER(drugs.drug_name) = 'drugalin' AND drugs.drug_id = treatments.drug_id
JOIN patients AS patients
  ON patients.patient_id = treatments.patient_id

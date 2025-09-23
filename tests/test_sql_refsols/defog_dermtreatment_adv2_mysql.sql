SELECT
  SUM(patients.weight_kg) / SUM(CASE WHEN NOT patients.weight_kg IS NULL THEN 1 ELSE 0 END) AS avg_weight
FROM main.treatments AS treatments
JOIN main.drugs AS drugs
  ON LOWER(drugs.drug_name) = 'drugalin' AND drugs.drug_id = treatments.drug_id
JOIN main.patients AS patients
  ON patients.patient_id = treatments.patient_id

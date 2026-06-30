SELECT
  AVG(patients.weight_kg) AS avg_weight
FROM defog.dermtreatment.treatments AS treatments
JOIN defog.dermtreatment.drugs AS drugs
  ON LOWER(drugs.drug_name) = 'drugalin' AND drugs.drug_id = treatments.drug_id
JOIN defog.dermtreatment.patients AS patients
  ON patients.patient_id = treatments.patient_id

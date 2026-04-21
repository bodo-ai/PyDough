SELECT
  AVG(patients.weight_kg) AS avg_weight
FROM cassandra.defog.treatments AS treatments
JOIN postgres.main.drugs AS drugs
  ON LOWER(drugs.drug_name) = 'drugalin' AND drugs.drug_id = treatments.drug_id
JOIN cassandra.defog.patients AS patients
  ON patients.patient_id = treatments.patient_id

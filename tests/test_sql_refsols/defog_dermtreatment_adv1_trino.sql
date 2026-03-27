SELECT DISTINCT
  doctors.loc_state AS state
FROM cassandra.defog.treatments AS treatments
JOIN postgres.main.drugs AS drugs
  ON drugs.drug_id = treatments.drug_id AND drugs.drug_type = 'biologic'
JOIN mongo.defog.doctors AS doctors
  ON doctors.doc_id = treatments.doc_id

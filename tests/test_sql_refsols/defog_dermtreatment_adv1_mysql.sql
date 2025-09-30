SELECT DISTINCT
  doctors.loc_state AS state
FROM main.treatments AS treatments
JOIN main.drugs AS drugs
  ON drugs.drug_id = treatments.drug_id AND drugs.drug_type = 'biologic'
JOIN main.doctors AS doctors
  ON doctors.doc_id = treatments.doc_id

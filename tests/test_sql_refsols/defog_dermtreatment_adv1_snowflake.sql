SELECT DISTINCT
  doctors_2.loc_state AS state
FROM main.doctors AS doctors
JOIN main.treatments AS treatments
  ON doctors.doc_id = treatments.doc_id
JOIN main.drugs AS drugs
  ON drugs.drug_id = treatments.drug_id AND drugs.drug_type = 'biologic'
JOIN main.doctors AS doctors_2
  ON doctors_2.doc_id = treatments.doc_id

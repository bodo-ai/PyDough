SELECT DISTINCT
  doctors.loc_state AS state
FROM treatments AS treatments
JOIN drugs AS drugs
  ON drugs.drug_id = treatments.drug_id AND drugs.drug_type = 'biologic'
JOIN doctors AS doctors
  ON doctors.doc_id = treatments.doc_id

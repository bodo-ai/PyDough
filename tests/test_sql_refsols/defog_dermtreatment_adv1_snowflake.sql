SELECT DISTINCT
  doctors.loc_state AS state
FROM dermtreatment.treatments AS treatments
JOIN dermtreatment.drugs AS drugs
  ON drugs.drug_id = treatments.drug_id AND drugs.drug_type = 'biologic'
JOIN dermtreatment.doctors AS doctors
  ON doctors.doc_id = treatments.doc_id

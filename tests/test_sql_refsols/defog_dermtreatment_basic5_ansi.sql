SELECT
  doctors.doc_id,
  doctors.first_name,
  doctors.last_name
FROM main.doctors AS doctors
JOIN main.treatments AS treatments
  ON doctors.doc_id = treatments.doc_id

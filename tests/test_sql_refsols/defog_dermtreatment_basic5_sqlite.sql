WITH _s1 AS (
  SELECT DISTINCT
    doc_id
  FROM main.treatments
)
SELECT
  doctors.doc_id,
  doctors.first_name,
  doctors.last_name
FROM main.doctors AS doctors
JOIN _s1 AS _s1
  ON _s1.doc_id = doctors.doc_id

SELECT
  doc_id,
  first_name,
  last_name
FROM doctors
WHERE
  EXISTS(
    SELECT
      1 AS `1`
    FROM treatments
    WHERE
      doctors.doc_id = doc_id
  )

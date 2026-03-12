WITH _u_0 AS (
  SELECT
    doc_id AS _u_1
  FROM dermtreatment.treatments
  GROUP BY
    1
)
SELECT
  doctors.doc_id,
  doctors.first_name,
  doctors.last_name
FROM dermtreatment.doctors AS doctors
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = doctors.doc_id
WHERE
  NOT _u_0._u_1 IS NULL

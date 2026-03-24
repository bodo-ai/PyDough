WITH "_u_0" AS (
  SELECT
    doc_id AS "_u_1"
  FROM MAIN.TREATMENTS
  GROUP BY
    doc_id
)
SELECT
  DOCTORS.doc_id,
  DOCTORS.first_name,
  DOCTORS.last_name
FROM MAIN.DOCTORS DOCTORS
LEFT JOIN "_u_0" "_u_0"
  ON DOCTORS.doc_id = "_u_0"."_u_1"
WHERE
  NOT "_u_0"."_u_1" IS NULL

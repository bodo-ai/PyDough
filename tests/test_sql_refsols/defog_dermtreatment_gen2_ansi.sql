WITH _t1 AS (
  SELECT
    doc_id,
    start_dt,
    treatment_id
  FROM main.treatments
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY start_dt NULLS LAST) = 1
)
SELECT
  doctors.last_name,
  doctors.year_reg,
  _t1.start_dt AS first_treatment_date,
  _t1.treatment_id AS first_treatment_id
FROM main.doctors AS doctors
JOIN _t1 AS _t1
  ON _t1.doc_id = doctors.doc_id
WHERE
  doctors.year_reg = EXTRACT(YEAR FROM DATE_SUB(CURRENT_TIMESTAMP(), 2, YEAR))

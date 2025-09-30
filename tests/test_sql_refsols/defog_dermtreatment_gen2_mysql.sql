WITH _t AS (
  SELECT
    doc_id,
    start_dt,
    treatment_id,
    ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY CASE WHEN start_dt IS NULL THEN 1 ELSE 0 END, start_dt) AS _w
  FROM main.treatments
)
SELECT
  doctors.last_name,
  doctors.year_reg,
  _t.start_dt AS first_treatment_date,
  _t.treatment_id AS first_treatment_id
FROM main.doctors AS doctors
JOIN _t AS _t
  ON _t._w = 1 AND _t.doc_id = doctors.doc_id
WHERE
  doctors.year_reg = EXTRACT(YEAR FROM DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '2' YEAR))

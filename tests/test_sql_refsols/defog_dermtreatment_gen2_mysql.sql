WITH _t AS (
  SELECT
    doc_id,
    start_dt,
    treatment_id,
    ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY CASE WHEN start_dt IS NULL THEN 1 ELSE 0 END, start_dt) AS _w
  FROM main.treatments
), _s1 AS (
  SELECT
    doc_id,
    start_dt,
    treatment_id
  FROM _t
  WHERE
    _w = 1
)
SELECT
  doctors.last_name,
  doctors.year_reg,
  _s1.start_dt AS first_treatment_date,
  _s1.treatment_id AS first_treatment_id
FROM main.doctors AS doctors
LEFT JOIN _s1 AS _s1
  ON _s1.doc_id = doctors.doc_id
WHERE
  doctors.year_reg = EXTRACT(YEAR FROM DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '-2' YEAR))

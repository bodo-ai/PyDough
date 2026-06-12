WITH _u_0 AS (
  SELECT
    patient_id AS _u_1
  FROM main.treatments
  GROUP BY
    1
), _t1 AS (
  SELECT
    MIN(CAST(STRFTIME('%Y', treatments.start_dt) AS INTEGER)) AS min_year_start_dt
  FROM main.patients AS patients
  LEFT JOIN _u_0 AS _u_0
    ON _u_0._u_1 = patients.patient_id
  LEFT JOIN main.treatments AS treatments
    ON patients.patient_id = treatments.patient_id
  WHERE
    NOT _u_0._u_1 IS NULL
  GROUP BY
    patients.patient_id
), _t0 AS (
  SELECT
    min_year_start_dt,
    COUNT(*) AS n_rows
  FROM _t1
  GROUP BY
    1
)
SELECT
  CAST(min_year_start_dt AS TEXT) AS year,
  n_rows AS number_of_new_patients,
  n_rows - LAG(n_rows, 1) OVER (ORDER BY min_year_start_dt) AS npi
FROM _t0
ORDER BY
  1

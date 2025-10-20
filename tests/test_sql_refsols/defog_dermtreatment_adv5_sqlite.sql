WITH _u_0 AS (
  SELECT
    patient_id AS _u_1
  FROM main.treatments
  GROUP BY
    1
), _s3 AS (
  SELECT
    patient_id,
    start_dt
  FROM main.treatments
), _t1 AS (
  SELECT
    MIN(CAST(STRFTIME('%Y', _s3.start_dt) AS INTEGER)) AS min_year_start_dt
  FROM main.patients AS patients
  LEFT JOIN _u_0 AS _u_0
    ON _u_0._u_1 = patients.patient_id
  LEFT JOIN _s3 AS _s3
    ON _s3.patient_id = patients.patient_id
  WHERE
    NOT _u_0._u_1 IS NULL
  GROUP BY
    _s3.patient_id
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
  CASE
    WHEN (
      n_rows - COALESCE(LAG(n_rows, 1) OVER (ORDER BY min_year_start_dt), n_rows)
    ) <> 0
    THEN n_rows - COALESCE(LAG(n_rows, 1) OVER (ORDER BY min_year_start_dt), n_rows)
    ELSE NULL
  END AS npi
FROM _t0
ORDER BY
  1

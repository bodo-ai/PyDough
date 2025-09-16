WITH _s3 AS (
  SELECT
    patient_id,
    MIN(EXTRACT(YEAR FROM CAST(start_dt AS DATETIME))) AS min_year_start_dt
  FROM main.treatments
  GROUP BY
    1
), _t0 AS (
  SELECT
    _s3.min_year_start_dt,
    COUNT(*) AS n_rows
  FROM main.patients AS patients
  JOIN main.treatments AS treatments
    ON patients.patient_id = treatments.patient_id
  LEFT JOIN _s3 AS _s3
    ON _s3.patient_id = patients.patient_id
  GROUP BY
    1
)
SELECT
  CAST(min_year_start_dt AS TEXT) AS year,
  n_rows AS number_of_new_patients,
  CASE
    WHEN (
      n_rows - COALESCE(LAG(n_rows, 1) OVER (ORDER BY min_year_start_dt NULLS LAST), n_rows)
    ) <> 0
    THEN n_rows - COALESCE(LAG(n_rows, 1) OVER (ORDER BY min_year_start_dt NULLS LAST), n_rows)
    ELSE NULL
  END AS npi
FROM _t0
ORDER BY
  1

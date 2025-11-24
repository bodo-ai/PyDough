WITH _t1 AS (
  SELECT
    MIN(EXTRACT(YEAR FROM CAST(treatments_2.start_dt AS DATETIME))) AS min_year_start_dt
  FROM main.patients AS patients
  JOIN main.treatments AS treatments
    ON patients.patient_id = treatments.patient_id
  LEFT JOIN main.treatments AS treatments_2
    ON patients.patient_id = treatments_2.patient_id
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

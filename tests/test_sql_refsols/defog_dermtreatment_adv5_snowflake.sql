WITH _u_0 AS (
  SELECT
    patient_id AS _u_1
  FROM main.treatments
  GROUP BY
    1
), _s3 AS (
  SELECT
    patient_id,
    MIN(YEAR(CAST(start_dt AS TIMESTAMP))) AS min_year_start_dt
  FROM main.treatments
  GROUP BY
    1
), _t0 AS (
  SELECT
    _s3.min_year_start_dt,
    COUNT(*) AS n_rows
  FROM main.patients AS patients
  LEFT JOIN _u_0 AS _u_0
    ON _u_0._u_1 = patients.patient_id
  LEFT JOIN _s3 AS _s3
    ON _s3.patient_id = patients.patient_id
  WHERE
    NOT _u_0._u_1 IS NULL
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
  1 NULLS FIRST

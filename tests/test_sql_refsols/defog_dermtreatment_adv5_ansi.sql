WITH _s3 AS (
  SELECT
    MIN(EXTRACT(YEAR FROM CAST(start_dt AS DATETIME))) AS min_start_year,
    patient_id
  FROM main.treatments
  GROUP BY
    2
), _t0 AS (
  SELECT
    COUNT(*) AS number_of_new_patients,
    _s3.min_start_year
  FROM main.patients AS patients
  JOIN main.treatments AS treatments
    ON patients.patient_id = treatments.patient_id
  LEFT JOIN _s3 AS _s3
    ON _s3.patient_id = patients.patient_id
  GROUP BY
    2
)
SELECT
  CAST(min_start_year AS TEXT) AS year,
  number_of_new_patients,
  CASE
    WHEN (
      number_of_new_patients - COALESCE(
        LAG(number_of_new_patients, 1) OVER (ORDER BY min_start_year NULLS LAST),
        number_of_new_patients
      )
    ) <> 0
    THEN number_of_new_patients - COALESCE(
      LAG(number_of_new_patients, 1) OVER (ORDER BY min_start_year NULLS LAST),
      number_of_new_patients
    )
    ELSE NULL
  END AS npi
FROM _t0
ORDER BY
  1

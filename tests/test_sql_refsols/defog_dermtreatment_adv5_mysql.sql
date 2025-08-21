WITH _u_0 AS (
  SELECT
    patient_id AS _u_1
  FROM main.treatments
  GROUP BY
    1
), _s3 AS (
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
  LEFT JOIN _u_0 AS _u_0
    ON _u_0._u_1 = patients.patient_id
  LEFT JOIN _s3 AS _s3
    ON _s3.patient_id = patients.patient_id
  WHERE
    NOT _u_0._u_1 IS NULL
  GROUP BY
    2
)
SELECT
  CAST(min_start_year AS CHAR) COLLATE utf8mb4_bin AS year,
  number_of_new_patients,
  CASE
    WHEN (
      number_of_new_patients - COALESCE(
        LAG(number_of_new_patients, 1) OVER (ORDER BY CASE WHEN min_start_year IS NULL THEN 1 ELSE 0 END, min_start_year),
        number_of_new_patients
      )
    ) <> 0
    THEN number_of_new_patients - COALESCE(
      LAG(number_of_new_patients, 1) OVER (ORDER BY CASE WHEN min_start_year IS NULL THEN 1 ELSE 0 END, min_start_year),
      number_of_new_patients
    )
    ELSE NULL
  END AS npi
FROM _t0
ORDER BY
  1

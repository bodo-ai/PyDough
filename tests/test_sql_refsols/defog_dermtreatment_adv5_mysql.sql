WITH _t1 AS (
  SELECT
    MIN(EXTRACT(YEAR FROM CAST(start_dt AS DATETIME))) AS min_year_start_dt
  FROM main.treatments
  GROUP BY
    patient_id
), _t0 AS (
  SELECT
    min_year_start_dt,
    COUNT(*) AS n_rows
  FROM _t1
  GROUP BY
    1
)
SELECT
  CAST(min_year_start_dt AS CHAR) COLLATE utf8mb4_bin AS year,
  n_rows AS number_of_new_patients,
  CASE
    WHEN (
      n_rows - COALESCE(
        LAG(n_rows, 1) OVER (ORDER BY CASE WHEN min_year_start_dt IS NULL THEN 1 ELSE 0 END, min_year_start_dt),
        n_rows
      )
    ) <> 0
    THEN n_rows - COALESCE(
      LAG(n_rows, 1) OVER (ORDER BY CASE WHEN min_year_start_dt IS NULL THEN 1 ELSE 0 END, min_year_start_dt),
      n_rows
    )
    ELSE NULL
  END AS npi
FROM _t0
ORDER BY
  1

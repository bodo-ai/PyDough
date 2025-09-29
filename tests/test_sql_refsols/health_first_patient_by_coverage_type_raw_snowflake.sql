WITH _t3 AS (
  SELECT
    protected_patients.date_of_birth,
    protected_patients.first_name,
    insurance_plans.insurance_plan_id,
    protected_patients.last_name
  FROM bodo.health.insurance_plans AS insurance_plans
  JOIN bodo.health.protected_patients AS protected_patients
    ON insurance_plans.insurance_plan_id = protected_patients.insurance_plan_id
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY coverage_type ORDER BY protected_patients.date_of_birth, protected_patients.patient_id) = 1
), _s3 AS (
  SELECT
    insurance_plan_id,
    ANY_VALUE(date_of_birth) AS anything_date_of_birth,
    ANY_VALUE(first_name) AS anything_first_name,
    ANY_VALUE(last_name) AS anything_last_name,
    COUNT(*) AS n_rows
  FROM _t3
  GROUP BY
    1
), _t0 AS (
  SELECT
    insurance_plans.coverage_type,
    MAX(_s3.anything_date_of_birth) AS max_anything_date_of_birth,
    MAX(_s3.anything_first_name) AS max_anything_first_name,
    MAX(_s3.anything_last_name) AS max_anything_last_name,
    SUM(_s3.n_rows) AS sum_n_rows
  FROM bodo.health.insurance_plans AS insurance_plans
  LEFT JOIN _s3 AS _s3
    ON _s3.insurance_plan_id = insurance_plans.insurance_plan_id
  GROUP BY
    1
)
SELECT
  coverage_type,
  max_anything_first_name AS first_name,
  max_anything_last_name AS last_name,
  max_anything_date_of_birth AS date_of_birth
FROM _t0
WHERE
  sum_n_rows > 0
ORDER BY
  1 NULLS FIRST

WITH _t1 AS (
  SELECT
    ANY_VALUE(treatments.diag_id) AS anything_diag_id,
    ANY_VALUE(treatments.patient_id) AS anything_patient_id,
    MAX(outcomes.day100_itch_vas) AS max_day100_itch_vas
  FROM main.treatments AS treatments
  JOIN main.outcomes AS outcomes
    ON outcomes.treatment_id = treatments.treatment_id
  GROUP BY
    outcomes.treatment_id
), _s3 AS (
  SELECT
    anything_diag_id,
    MAX(max_day100_itch_vas) AS max_max_day100_itch_vas,
    COUNT(DISTINCT anything_patient_id) AS ndistinct_anything_patient_id
  FROM _t1
  GROUP BY
    1
)
SELECT
  diagnoses.diag_name AS diagnosis_name,
  _s3.ndistinct_anything_patient_id AS num_patients,
  _s3.max_max_day100_itch_vas AS max_itch_score
FROM main.diagnoses AS diagnoses
JOIN _s3 AS _s3
  ON _s3.anything_diag_id = diagnoses.diag_id
ORDER BY
  3 DESC,
  2 DESC
LIMIT 3

WITH _s1 AS (
  SELECT
    MAX(day100_itch_vas) AS max_day100_itch_vas,
    treatment_id
  FROM main.outcomes
  GROUP BY
    2
), _s3 AS (
  SELECT
    MAX(_s1.max_day100_itch_vas) AS max_itch_score,
    COUNT(DISTINCT treatments.patient_id) AS num_patients,
    treatments.diag_id
  FROM main.treatments AS treatments
  JOIN _s1 AS _s1
    ON _s1.treatment_id = treatments.treatment_id
  GROUP BY
    3
)
SELECT
  diagnoses.diag_name AS diagnosis_name,
  _s3.num_patients,
  _s3.max_itch_score
FROM main.diagnoses AS diagnoses
JOIN _s3 AS _s3
  ON _s3.diag_id = diagnoses.diag_id
ORDER BY
  3 DESC NULLS LAST,
  2 DESC NULLS LAST
LIMIT 3

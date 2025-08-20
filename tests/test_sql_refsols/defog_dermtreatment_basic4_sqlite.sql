WITH _t2 AS (
  SELECT
    day100_itch_vas,
    treatment_id
  FROM main.outcomes
  WHERE
    NOT day100_itch_vas IS NULL
), _s3 AS (
  SELECT
    MAX(_t2.day100_itch_vas) AS max_day100_itch_vas,
    treatments.diag_id
  FROM main.treatments AS treatments
  JOIN _t2 AS _t2
    ON _t2.treatment_id = treatments.treatment_id
  GROUP BY
    treatments.diag_id
), _s5 AS (
  SELECT DISTINCT
    treatment_id
  FROM _t2
), _s7 AS (
  SELECT
    COUNT(DISTINCT treatments.patient_id) AS ndistinct_patient_id,
    treatments.diag_id
  FROM main.treatments AS treatments
  JOIN _s5 AS _s5
    ON _s5.treatment_id = treatments.treatment_id
  GROUP BY
    treatments.diag_id
)
SELECT
  diagnoses.diag_name AS diagnosis_name,
  COALESCE(_s7.ndistinct_patient_id, 0) AS num_patients,
  _s3.max_day100_itch_vas AS max_itch_score
FROM main.diagnoses AS diagnoses
JOIN _s3 AS _s3
  ON _s3.diag_id = diagnoses.diag_id
LEFT JOIN _s7 AS _s7
  ON _s7.diag_id = diagnoses.diag_id
ORDER BY
  _s3.max_day100_itch_vas DESC
LIMIT 3

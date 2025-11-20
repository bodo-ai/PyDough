WITH _s1 AS (
  SELECT
    treatment_id,
    MAX(day100_itch_vas) AS max_day100itchvas
  FROM main.outcomes
  GROUP BY
    1
), _s3 AS (
  SELECT
    treatments.diag_id,
    MAX(_s1.max_day100itchvas) AS max_maxday100itchvas,
    COUNT(DISTINCT treatments.patient_id) AS ndistinct_patientid
  FROM main.treatments AS treatments
  JOIN _s1 AS _s1
    ON _s1.treatment_id = treatments.treatment_id
  GROUP BY
    1
)
SELECT
  diagnoses.diag_name AS diagnosis_name,
  _s3.ndistinct_patientid AS num_patients,
  _s3.max_maxday100itchvas AS max_itch_score
FROM main.diagnoses AS diagnoses
JOIN _s3 AS _s3
  ON _s3.diag_id = diagnoses.diag_id
ORDER BY
  3 DESC NULLS LAST,
  2 DESC NULLS LAST
LIMIT 3

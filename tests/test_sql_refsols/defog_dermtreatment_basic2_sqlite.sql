WITH _s1 AS (
  SELECT
    ins_type,
    patient_id
  FROM main.patients
), _s6 AS (
  SELECT
    _s1.ins_type,
    COUNT(DISTINCT treatments.patient_id) AS ndistinct_patient_id
  FROM main.treatments AS treatments
  JOIN _s1 AS _s1
    ON _s1.patient_id = treatments.patient_id
  WHERE
    CAST(STRFTIME('%Y', treatments.end_dt) AS INTEGER) = 2022
  GROUP BY
    1
), _s5 AS (
  SELECT
    treatment_id,
    COUNT(day100_pasi_score) AS count_day100_pasi_score,
    SUM(day100_pasi_score) AS sum_day100_pasi_score
  FROM main.outcomes
  GROUP BY
    1
), _s7 AS (
  SELECT
    _s3.ins_type,
    SUM(_s5.count_day100_pasi_score) AS sum_count_day100_pasi_score,
    SUM(_s5.sum_day100_pasi_score) AS sum_sum_day100_pasi_score
  FROM main.treatments AS treatments
  JOIN _s1 AS _s3
    ON _s3.patient_id = treatments.patient_id
  JOIN _s5 AS _s5
    ON _s5.treatment_id = treatments.treatment_id
  WHERE
    CAST(STRFTIME('%Y', treatments.end_dt) AS INTEGER) = 2022
  GROUP BY
    1
)
SELECT
  _s6.ins_type AS insurance_type,
  _s6.ndistinct_patient_id AS num_distinct_patients,
  CAST(_s7.sum_sum_day100_pasi_score AS REAL) / _s7.sum_count_day100_pasi_score AS avg_pasi_score_day100
FROM _s6 AS _s6
JOIN _s7 AS _s7
  ON _s6.ins_type = _s7.ins_type
ORDER BY
  3
LIMIT 5

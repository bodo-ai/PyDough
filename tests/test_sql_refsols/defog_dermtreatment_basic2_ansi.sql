WITH _t2 AS (
  SELECT
    end_dt,
    patient_id,
    treatment_id
  FROM main.treatments
  WHERE
    EXTRACT(YEAR FROM CAST(end_dt AS DATETIME)) = 2022
), _t3 AS (
  SELECT
    day100_pasi_score,
    treatment_id
  FROM main.outcomes
  WHERE
    NOT day100_pasi_score IS NULL
), _s3 AS (
  SELECT
    ins_type,
    patient_id
  FROM main.patients
), _s10 AS (
  SELECT
    _s3.ins_type,
    COUNT(DISTINCT _t2.patient_id) AS ndistinct_patient_id
  FROM _t2 AS _t2
  JOIN _t3 AS _t3
    ON _t2.treatment_id = _t3.treatment_id
  JOIN _s3 AS _s3
    ON _s3.patient_id = _t2.patient_id
  GROUP BY
    1
), _s9 AS (
  SELECT
    treatment_id,
    COUNT(day100_pasi_score) AS count_day100_pasi_score,
    SUM(day100_pasi_score) AS sum_day100_pasi_score
  FROM main.outcomes
  GROUP BY
    1
), _s11 AS (
  SELECT
    SUM(_s9.sum_day100_pasi_score) / SUM(_s9.count_day100_pasi_score) AS sum_sum_day100_pasi_score_div_sum_count_day100_pasi_score,
    _s7.ins_type
  FROM _t2 AS _t6
  JOIN _t3 AS _t7
    ON _t6.treatment_id = _t7.treatment_id
  JOIN _s3 AS _s7
    ON _s7.patient_id = _t6.patient_id
  JOIN _s9 AS _s9
    ON _s9.treatment_id = _t6.treatment_id
  GROUP BY
    2
)
SELECT
  _s10.ins_type AS insurance_type,
  _s10.ndistinct_patient_id AS num_distinct_patients,
  _s11.sum_sum_day100_pasi_score_div_sum_count_day100_pasi_score AS avg_pasi_score_day100
FROM _s10 AS _s10
LEFT JOIN _s11 AS _s11
  ON _s10.ins_type = _s11.ins_type
ORDER BY
  3
LIMIT 5

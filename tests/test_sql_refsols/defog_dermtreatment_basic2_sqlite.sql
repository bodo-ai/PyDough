WITH _t2 AS (
  SELECT
    end_dt,
    patient_id,
    treatment_id
  FROM main.treatments
  WHERE
    CAST(STRFTIME('%Y', end_dt) AS INTEGER) = 2022
), _t3 AS (
  SELECT
    day100_pasi_score,
    treatment_id
  FROM main.outcomes
  WHERE
    NOT day100_pasi_score IS NULL
), _u_0 AS (
  SELECT
    treatment_id AS _u_1
  FROM _t3
  GROUP BY
    1
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
  LEFT JOIN _u_0 AS _u_0
    ON _t2.treatment_id = _u_0._u_1
  JOIN _s3 AS _s3
    ON _s3.patient_id = _t2.patient_id
  WHERE
    NOT _u_0._u_1 IS NULL
  GROUP BY
    1
), _u_2 AS (
  SELECT
    treatment_id AS _u_3
  FROM _t3
  GROUP BY
    1
), _s11 AS (
  SELECT
    _s7.ins_type,
    AVG(outcomes.day100_pasi_score) AS avg_day100_pasi_score
  FROM _t2 AS _t5
  LEFT JOIN _u_2 AS _u_2
    ON _t5.treatment_id = _u_2._u_3
  JOIN _s3 AS _s7
    ON _s7.patient_id = _t5.patient_id
  JOIN main.outcomes AS outcomes
    ON _t5.treatment_id = outcomes.treatment_id
  WHERE
    NOT _u_2._u_3 IS NULL
  GROUP BY
    1
)
SELECT
  _s10.ins_type AS insurance_type,
  _s10.ndistinct_patient_id AS num_distinct_patients,
  _s11.avg_day100_pasi_score AS avg_pasi_score_day100
FROM _s10 AS _s10
LEFT JOIN _s11 AS _s11
  ON _s10.ins_type = _s11.ins_type
ORDER BY
  3
LIMIT 5

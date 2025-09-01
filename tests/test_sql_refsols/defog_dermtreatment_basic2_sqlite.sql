WITH _t1 AS (
  SELECT
    end_dt,
    patient_id,
    treatment_id
  FROM main.treatments
  WHERE
    CAST(STRFTIME('%Y', end_dt) AS INTEGER) = 2022
), _t2 AS (
  SELECT
    day100_pasi_score,
    treatment_id
  FROM main.outcomes
  WHERE
    NOT day100_pasi_score IS NULL
), _u_0 AS (
  SELECT
    treatment_id AS _u_1
  FROM _t2
  GROUP BY
    1
), _s3 AS (
  SELECT
    ins_type,
    patient_id
  FROM main.patients
), _s10 AS (
  SELECT DISTINCT
    _s3.ins_type
  FROM _t1 AS _t1
  LEFT JOIN _u_0 AS _u_0
    ON _t1.treatment_id = _u_0._u_1
  JOIN _s3 AS _s3
    ON _s3.patient_id = _t1.patient_id
  WHERE
    NOT _u_0._u_1 IS NULL
), _u_2 AS (
  SELECT
    treatment_id AS _u_3
  FROM _t2
  GROUP BY
    1
), _s11 AS (
  SELECT
    COUNT(DISTINCT patients.patient_id) AS ndistinct_patient_id,
    _s7.ins_type
  FROM _t1 AS _t4
  LEFT JOIN _u_2 AS _u_2
    ON _t4.treatment_id = _u_2._u_3
  JOIN _s3 AS _s7
    ON _s7.patient_id = _t4.patient_id
  JOIN main.patients AS patients
    ON _t4.patient_id = patients.patient_id
  WHERE
    NOT _u_2._u_3 IS NULL
  GROUP BY
    2
), _u_4 AS (
  SELECT
    treatment_id AS _u_5
  FROM _t2
  GROUP BY
    1
), _s19 AS (
  SELECT
    AVG(outcomes.day100_pasi_score) AS avg_day100_pasi_score,
    _s15.ins_type
  FROM _t1 AS _t7
  LEFT JOIN _u_4 AS _u_4
    ON _t7.treatment_id = _u_4._u_5
  JOIN _s3 AS _s15
    ON _s15.patient_id = _t7.patient_id
  JOIN main.outcomes AS outcomes
    ON _t7.treatment_id = outcomes.treatment_id
  WHERE
    NOT _u_4._u_5 IS NULL
  GROUP BY
    2
)
SELECT
  _s10.ins_type AS insurance_type,
  _s11.ndistinct_patient_id AS num_distinct_patients,
  _s19.avg_day100_pasi_score AS avg_pasi_score_day100
FROM _s10 AS _s10
JOIN _s11 AS _s11
  ON _s10.ins_type = _s11.ins_type
LEFT JOIN _s19 AS _s19
  ON _s10.ins_type = _s19.ins_type
ORDER BY
  3
LIMIT 5

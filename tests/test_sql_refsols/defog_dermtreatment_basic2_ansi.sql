WITH _t1 AS (
  SELECT
    end_dt,
    patient_id,
    treatment_id
  FROM main.treatments
  WHERE
    EXTRACT(YEAR FROM CAST(end_dt AS DATETIME)) = 2022
), _t2 AS (
  SELECT
    day100_pasi_score,
    treatment_id
  FROM main.outcomes
  WHERE
    NOT day100_pasi_score IS NULL
), _s1 AS (
  SELECT DISTINCT
    treatment_id
  FROM _t2
), _s3 AS (
  SELECT
    ins_type,
    patient_id
  FROM main.patients
), _s10 AS (
  SELECT DISTINCT
    _s3.ins_type
  FROM _t1 AS _t1
  JOIN _s1 AS _s1
    ON _s1.treatment_id = _t1.treatment_id
  JOIN _s3 AS _s3
    ON _s3.patient_id = _t1.patient_id
), _s5 AS (
  SELECT DISTINCT
    treatment_id
  FROM _t2
), _s11 AS (
  SELECT
    COUNT(DISTINCT patients.patient_id) AS ndistinct_patient_id,
    _s7.ins_type
  FROM _t1 AS _t4
  JOIN _s5 AS _s5
    ON _s5.treatment_id = _t4.treatment_id
  JOIN _s3 AS _s7
    ON _s7.patient_id = _t4.patient_id
  JOIN main.patients AS patients
    ON _t4.patient_id = patients.patient_id
  GROUP BY
    _s7.ins_type
), _s13 AS (
  SELECT DISTINCT
    treatment_id
  FROM _t2
), _s19 AS (
  SELECT
    AVG(outcomes.day100_pasi_score) AS avg_day100_pasi_score,
    _s15.ins_type
  FROM _t1 AS _t7
  JOIN _s13 AS _s13
    ON _s13.treatment_id = _t7.treatment_id
  JOIN _s3 AS _s15
    ON _s15.patient_id = _t7.patient_id
  JOIN main.outcomes AS outcomes
    ON _t7.treatment_id = outcomes.treatment_id
  GROUP BY
    _s15.ins_type
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
  _s19.avg_day100_pasi_score
LIMIT 5

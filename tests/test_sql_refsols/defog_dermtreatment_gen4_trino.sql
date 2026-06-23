SELECT
  treatments_2.treatment_id,
  treatments_2.start_dt AS treatment_start_date,
  treatments_2.end_dt AS treatment_end_date,
  concomitant_meds.start_dt AS concomitant_med_start_date,
  concomitant_meds.end_dt AS concomitant_med_end_date
FROM cassandra.defog.treatments AS treatments
JOIN mongo.defog.concomitant_meds AS concomitant_meds
  ON concomitant_meds.treatment_id = treatments.treatment_id
JOIN cassandra.defog.treatments AS treatments_2
  ON DATE_DIFF(
    'DAY',
    CAST(DATE_TRUNC('DAY', CAST(treatments_2.start_dt AS TIMESTAMP)) AS TIMESTAMP),
    CAST(DATE_TRUNC('DAY', CAST(concomitant_meds.start_dt AS TIMESTAMP)) AS TIMESTAMP)
  ) <= 14
  AND concomitant_meds.treatment_id = treatments_2.treatment_id
  AND treatments_2.is_placebo
WHERE
  treatments.is_placebo = TRUE

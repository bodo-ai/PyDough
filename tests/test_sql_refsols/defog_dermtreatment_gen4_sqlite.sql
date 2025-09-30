SELECT
  treatments.treatment_id,
  treatments.start_dt AS treatment_start_date,
  treatments.end_dt AS treatment_end_date,
  concomitant_meds.start_dt AS concomitant_med_start_date,
  concomitant_meds.end_dt AS concomitant_med_end_date
FROM main.concomitant_meds AS concomitant_meds
JOIN main.treatments AS treatments
  ON CAST((
    JULIANDAY(DATE(concomitant_meds.start_dt, 'start of day')) - JULIANDAY(DATE(treatments.start_dt, 'start of day'))
  ) AS INTEGER) <= 14
  AND concomitant_meds.treatment_id = treatments.treatment_id
  AND treatments.is_placebo

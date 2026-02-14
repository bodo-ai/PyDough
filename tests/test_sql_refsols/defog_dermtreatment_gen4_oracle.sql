SELECT
  TREATMENTS_2.treatment_id,
  TREATMENTS_2.start_dt AS treatment_start_date,
  TREATMENTS_2.end_dt AS treatment_end_date,
  CONCOMITANT_MEDS.start_dt AS concomitant_med_start_date,
  CONCOMITANT_MEDS.end_dt AS concomitant_med_end_date
FROM MAIN.TREATMENTS TREATMENTS
JOIN MAIN.CONCOMITANT_MEDS CONCOMITANT_MEDS
  ON CONCOMITANT_MEDS.treatment_id = TREATMENTS.treatment_id
JOIN MAIN.TREATMENTS TREATMENTS_2
  ON CONCOMITANT_MEDS.treatment_id = TREATMENTS_2.treatment_id
  AND DATEDIFF(
    CAST(CONCOMITANT_MEDS.start_dt AS DATETIME),
    CAST(TREATMENTS_2.start_dt AS DATETIME),
    DAY
  ) <= 14
  AND TREATMENTS_2.is_placebo
WHERE
  TREATMENTS.is_placebo = TRUE

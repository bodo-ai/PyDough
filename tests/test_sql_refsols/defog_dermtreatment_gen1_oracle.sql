SELECT
  ADVERSE_EVENTS.treatment_id,
  TREATMENTS.start_dt AS treatment_start_date,
  ADVERSE_EVENTS.reported_dt AS adverse_event_date,
  ADVERSE_EVENTS.description
FROM MAIN.ADVERSE_EVENTS ADVERSE_EVENTS
JOIN MAIN.TREATMENTS TREATMENTS
  ON (
    CAST(ADVERSE_EVENTS.reported_dt AS DATE) - CAST(TREATMENTS.start_dt AS DATE)
  ) <= 10
  AND ADVERSE_EVENTS.treatment_id = TREATMENTS.treatment_id

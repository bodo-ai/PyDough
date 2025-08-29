SELECT
  adverse_events.treatment_id,
  treatments.start_dt AS treatment_start_date,
  adverse_events.reported_dt AS adverse_event_date,
  adverse_events.description
FROM main.adverse_events AS adverse_events
JOIN main.treatments AS treatments
  ON DATEDIFF(adverse_events.reported_dt, treatments.start_dt) <= 10
  AND adverse_events.treatment_id = treatments.treatment_id

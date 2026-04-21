SELECT
  adverse_events.treatment_id,
  treatments.start_dt AS treatment_start_date,
  adverse_events.reported_dt AS adverse_event_date,
  adverse_events.description
FROM cassandra.defog.adverse_events AS adverse_events
JOIN cassandra.defog.treatments AS treatments
  ON DATE_DIFF(
    'DAY',
    CAST(DATE_TRUNC('DAY', CAST(treatments.start_dt AS TIMESTAMP)) AS TIMESTAMP),
    CAST(DATE_TRUNC('DAY', CAST(adverse_events.reported_dt AS TIMESTAMP)) AS TIMESTAMP)
  ) <= 10
  AND adverse_events.treatment_id = treatments.treatment_id

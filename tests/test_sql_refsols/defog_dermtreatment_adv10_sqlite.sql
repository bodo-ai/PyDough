SELECT
  treatments.drug_id,
  drugs.drug_name,
  COUNT(*) AS num_adverse_events
FROM main.adverse_events AS adverse_events
JOIN main.treatments AS treatments
  ON CAST(STRFTIME('%Y', adverse_events.reported_dt) AS INTEGER) = CAST(STRFTIME('%Y', treatments.start_dt) AS INTEGER)
  AND CAST(STRFTIME('%m', adverse_events.reported_dt) AS INTEGER) = CAST(STRFTIME('%m', treatments.start_dt) AS INTEGER)
  AND adverse_events.treatment_id = treatments.treatment_id
JOIN main.drugs AS drugs
  ON drugs.drug_id = treatments.drug_id
GROUP BY
  treatments.drug_id,
  drugs.drug_name
ORDER BY
  num_adverse_events DESC
LIMIT 1

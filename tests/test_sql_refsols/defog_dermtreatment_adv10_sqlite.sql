SELECT
  treatments.drug_id,
  drugs.drug_name,
  COUNT(*) AS num_adverse_events
FROM main.adverse_events AS adverse_events
JOIN main.treatments AS treatments
  ON DATE(adverse_events.reported_dt, 'start of month') = DATE(treatments.start_dt, 'start of month')
  AND adverse_events.treatment_id = treatments.treatment_id
JOIN main.drugs AS drugs
  ON drugs.drug_id = treatments.drug_id
GROUP BY
  1,
  2
ORDER BY
  3 DESC
LIMIT 1

SELECT
  treatments.drug_id,
  drugs.drug_name,
  COUNT(*) AS num_adverse_events
FROM main.adverse_events AS adverse_events
JOIN main.treatments AS treatments
  ON EXTRACT(MONTH FROM CAST(adverse_events.reported_dt AS DATETIME)) = EXTRACT(MONTH FROM CAST(treatments.start_dt AS DATETIME))
  AND EXTRACT(YEAR FROM CAST(adverse_events.reported_dt AS DATETIME)) = EXTRACT(YEAR FROM CAST(treatments.start_dt AS DATETIME))
  AND adverse_events.treatment_id = treatments.treatment_id
JOIN main.drugs AS drugs
  ON drugs.drug_id = treatments.drug_id
GROUP BY
  treatments.drug_id,
  drugs.drug_name
ORDER BY
  num_adverse_events DESC
LIMIT 1

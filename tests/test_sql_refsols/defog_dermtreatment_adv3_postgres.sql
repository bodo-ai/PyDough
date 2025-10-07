SELECT
  adverse_events.description,
  adverse_events.treatment_id,
  drugs.drug_id,
  drugs.drug_name
FROM main.adverse_events AS adverse_events
JOIN main.treatments AS treatments
  ON adverse_events.treatment_id = treatments.treatment_id
JOIN main.drugs AS drugs
  ON drugs.drug_id = treatments.drug_id AND drugs.drug_type = 'topical'

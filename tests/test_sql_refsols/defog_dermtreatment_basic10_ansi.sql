SELECT
  drugs.drug_id,
  drugs.drug_name
FROM main.drugs AS drugs
JOIN main.treatments AS treatments
  ON drugs.drug_id = treatments.drug_id

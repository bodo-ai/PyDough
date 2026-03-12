SELECT
  drug_id,
  drug_name
FROM main.drugs
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.treatments
    WHERE
      drugs.drug_id = drug_id
  )

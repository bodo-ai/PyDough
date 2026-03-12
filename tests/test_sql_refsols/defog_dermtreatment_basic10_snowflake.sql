SELECT
  drug_id,
  drug_name
FROM dermtreatment.drugs
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM dermtreatment.treatments
    WHERE
      drugs.drug_id = drug_id
  )

SELECT
  drug_id,
  drug_name
FROM drugs
WHERE
  NOT EXISTS(
    SELECT
      1 AS `1`
    FROM treatments
    WHERE
      drugs.drug_id = drug_id
  )

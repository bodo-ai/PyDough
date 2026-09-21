WITH _s1 AS (
  SELECT
    drug_id
  FROM main.treatments
)
SELECT
  drugs.drug_id,
  drugs.drug_name
FROM main.drugs AS drugs
ANTI JOIN _s1 AS _s1
  ON _s1.drug_id = drugs.drug_id

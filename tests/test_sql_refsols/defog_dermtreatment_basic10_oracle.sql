WITH "_u_0" AS (
  SELECT
    drug_id AS "_u_1"
  FROM MAIN.TREATMENTS
  GROUP BY
    drug_id
)
SELECT
  DRUGS.drug_id,
  DRUGS.drug_name
FROM MAIN.DRUGS DRUGS
LEFT JOIN "_u_0" "_u_0"
  ON DRUGS.drug_id = "_u_0"."_u_1"
WHERE
  "_u_0"."_u_1" IS NULL

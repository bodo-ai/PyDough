WITH _u_0 AS (
  SELECT
    drug_id AS _u_1
  FROM treatments
  GROUP BY
    1
)
SELECT
  drugs.drug_id,
  drugs.drug_name
FROM drugs AS drugs
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = drugs.drug_id
WHERE
  _u_0._u_1 IS NULL

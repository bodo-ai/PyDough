WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    drug_id
  FROM main.treatments
  GROUP BY
    drug_id
)
SELECT
  drugs.drug_id,
  drugs.drug_name
FROM main.drugs AS drugs
LEFT JOIN _s1 AS _s1
  ON _s1.drug_id = drugs.drug_id
WHERE
  (
    _s1.n_rows = 0 OR _s1.n_rows IS NULL
  ) = 1

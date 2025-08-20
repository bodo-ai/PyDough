WITH _s1 AS (
  SELECT
    AVG(tot_drug_amt) AS avg_tot_drug_amt,
    COUNT(*) AS n_rows,
    drug_id
  FROM main.treatments
  GROUP BY
    drug_id
)
SELECT
  drugs.drug_name,
  COALESCE(_s1.n_rows, 0) AS num_treatments,
  _s1.avg_tot_drug_amt AS avg_drug_amount
FROM main.drugs AS drugs
LEFT JOIN _s1 AS _s1
  ON _s1.drug_id = drugs.drug_id
ORDER BY
  COALESCE(_s1.n_rows, 0) DESC,
  _s1.avg_tot_drug_amt DESC,
  drug_name
LIMIT 5

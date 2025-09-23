WITH _s1 AS (
  SELECT
    drug_id,
    AVG(CAST(tot_drug_amt AS DECIMAL)) AS avg_tot_drug_amt,
    COUNT(*) AS n_rows
  FROM main.treatments
  GROUP BY
    1
)
SELECT
  drugs.drug_name,
  COALESCE(_s1.n_rows, 0) AS num_treatments,
  _s1.avg_tot_drug_amt AS avg_drug_amount
FROM main.drugs AS drugs
LEFT JOIN _s1 AS _s1
  ON _s1.drug_id = drugs.drug_id
ORDER BY
  2 DESC NULLS LAST,
  3 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 5

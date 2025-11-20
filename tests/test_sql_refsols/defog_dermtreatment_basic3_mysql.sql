WITH _s1 AS (
  SELECT
    drug_id,
    AVG(tot_drug_amt) AS avg_totdrugamt,
    COUNT(*) AS n_rows
  FROM main.treatments
  GROUP BY
    1
)
SELECT
  drug_name COLLATE utf8mb4_bin AS drug_name,
  COALESCE(_s1.n_rows, 0) AS num_treatments,
  _s1.avg_totdrugamt AS avg_drug_amount
FROM main.drugs AS drugs
LEFT JOIN _s1 AS _s1
  ON _s1.drug_id = drugs.drug_id
ORDER BY
  2 DESC,
  3 DESC,
  1
LIMIT 5

WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(tot_drug_amt) AS sum_tot_drug_amt,
    doc_id
  FROM main.treatments
  WHERE
    DATEDIFF(MONTH, CAST(start_dt AS DATETIME), CURRENT_TIMESTAMP()) <= 6
  GROUP BY
    3
), _t1 AS (
  SELECT
    SUM(_s1.n_rows) AS sum_n_rows,
    SUM(_s1.sum_tot_drug_amt) AS sum_sum_tot_drug_amt,
    doctors.specialty
  FROM main.doctors AS doctors
  LEFT JOIN _s1 AS _s1
    ON _s1.doc_id = doctors.doc_id
  GROUP BY
    3
)
SELECT
  specialty,
  sum_n_rows AS num_treatments,
  COALESCE(sum_sum_tot_drug_amt, 0) AS total_drug_amount
FROM _t1
WHERE
  sum_n_rows > 0
ORDER BY
  3 DESC NULLS LAST
LIMIT 3

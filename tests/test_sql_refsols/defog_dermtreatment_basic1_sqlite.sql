WITH _s1 AS (
  SELECT
    doc_id,
    COUNT(*) AS n_rows,
    SUM(tot_drug_amt) AS sum_tot_drug_amt
  FROM main.treatments
  WHERE
    (
      (
        CAST(STRFTIME('%Y', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%Y', start_dt) AS INTEGER)
      ) * 12 + CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%m', start_dt) AS INTEGER)
    ) <= 6
  GROUP BY
    1
), _t1 AS (
  SELECT
    doctors.specialty,
    SUM(_s1.n_rows) AS sum_n_rows,
    SUM(_s1.sum_tot_drug_amt) AS sum_sum_tot_drug_amt
  FROM main.doctors AS doctors
  LEFT JOIN _s1 AS _s1
    ON _s1.doc_id = doctors.doc_id
  GROUP BY
    1
)
SELECT
  specialty,
  sum_n_rows AS num_treatments,
  COALESCE(sum_sum_tot_drug_amt, 0) AS total_drug_amount
FROM _t1
WHERE
  sum_n_rows > 0
ORDER BY
  3 DESC
LIMIT 3
